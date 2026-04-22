const mysql = require('mysql2/promise');
const archiver = require('archiver');
const https = require('https');
const http = require('http');
const { URL } = require('url');

// Sécurité : seulement ces domaines sont autorisés pour le téléchargement d'images
const ALLOWED_IMAGE_HOSTS = ['zaap.ovh'];

// Renommage possible
const VALID_RENAMES = new Set(['origin', 'ref', 'ean']);

// Parallélisme max pour télécharger les images d'UN chunk
const DOWNLOAD_CONCURRENCY = 8;

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = { status: 401, body: { error: 'Unauthorized' } };
    return;
  }

  // Paramètres
  const rename = VALID_RENAMES.has(req.query.rename) ? req.query.rename : 'ref';
  const chunkIndex = parseInt(req.query.chunk || '0', 10);
  const chunkSize = Math.min(parseInt(req.query.chunkSize || '100', 10), 100);

  // Mode 1 : liste explicite d'identifiants (refs, eans ou filenames)
  const refsList = parseList(req.query.refs);
  const eansList = parseList(req.query.eans);
  const filesList = parseList(req.query.files);

  // Mode 2 : filtres classiques (comme l'export)
  const search = (req.query.search || '').trim();
  const f = {
    fournisseurs: parseList(req.query.fournisseurs),
    marques: parseList(req.query.marques),
    licences: parseList(req.query.licences),
    actif: req.query.actif,
    statuts: parseList(req.query.statuts),
    cat_trad: parseList(req.query.cat_trad),
    sous_cat_trad: parseList(req.query.sous_cat_trad),
    cat_web: parseList(req.query.cat_web),
    sous_cat_web: parseList(req.query.sous_cat_web),
    acheteurs: parseList(req.query.acheteurs),
    rayons: parseList(req.query.rayons).map(r => parseInt(r, 10)).filter(n => !isNaN(n)),
    stock1_op: req.query.stock1_op, stock1_val: req.query.stock1_val,
    stock2_op: req.query.stock2_op, stock2_val: req.query.stock2_val,
    stock3_op: req.query.stock3_op, stock3_val: req.query.stock3_val,
  };

  let connection;
  try {
    connection = await mysql.createConnection({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE
    });

    // Récupérer la liste complète des articles concernés (REF_JACTAL, EAN_USINE, URL_PHOTO1)
    let allArticles = [];

    if (refsList.length || eansList.length || filesList.length) {
      // Mode liste : chercher par REF_JACTAL, EAN_USINE ou filename
      const conditions = [];
      const params = [];

      if (refsList.length) {
        conditions.push(`REF_JACTAL IN (${refsList.map(() => '?').join(',')})`);
        params.push(...refsList);
      }
      if (eansList.length) {
        conditions.push(`EAN_USINE IN (${eansList.map(() => '?').join(',')})`);
        params.push(...eansList);
      }
      if (filesList.length) {
        // Filename : on matche sur le nom terminant l'URL
        const likes = filesList.map(fn => `URL_PHOTO1 LIKE ?`).join(' OR ');
        conditions.push(`(${likes})`);
        filesList.forEach(fn => params.push('%/' + fn));
      }

      const whereSql = 'WHERE ' + conditions.join(' OR ');
      const [rows] = await connection.execute(
        `SELECT REF_JACTAL, EAN_USINE, URL_PHOTO1 FROM V_ARTICLES_CATALOGUE ${whereSql}`,
        params
      );
      allArticles = rows;

      // Rebâtir la liste complète pour calculer les "non trouvés"
      const foundRefs = new Set(allArticles.map(a => a.REF_JACTAL));
      const foundEans = new Set(allArticles.map(a => a.EAN_USINE).filter(Boolean));
      const foundFiles = new Set(
        allArticles
          .map(a => a.URL_PHOTO1 ? a.URL_PHOTO1.split('/').pop() : null)
          .filter(Boolean)
      );
      context.notFound = [];
      refsList.forEach(r => { if (!foundRefs.has(r)) context.notFound.push({ id: r, type: 'REF', reason: 'Article introuvable' }); });
      eansList.forEach(e => { if (!foundEans.has(e)) context.notFound.push({ id: e, type: 'EAN', reason: 'EAN introuvable' }); });
      filesList.forEach(fn => { if (!foundFiles.has(fn)) context.notFound.push({ id: fn, type: 'FILE', reason: 'Fichier introuvable' }); });
    } else {
      // Mode filtres
      const { where, params } = buildWhere(search, f);
      const [rows] = await connection.execute(
        `SELECT REF_JACTAL, EAN_USINE, URL_PHOTO1 FROM V_ARTICLES_CATALOGUE ${where} ORDER BY REF_JACTAL ASC`,
        params
      );
      allArticles = rows;
      context.notFound = [];
    }

    // Chunking : ne prendre que le slice demandé
    const totalArticles = allArticles.length;
    const start = chunkIndex * chunkSize;
    const end = start + chunkSize;
    const articlesInChunk = allArticles.slice(start, end);
    const totalChunks = Math.ceil(totalArticles / chunkSize);

    context.log(`images-zip chunk ${chunkIndex + 1}/${totalChunks} : ${articlesInChunk.length} articles`);

    // Filtrer ceux sans URL_PHOTO1
    const skippedNoPhoto = [];
    const articlesWithPhoto = [];
    for (const a of articlesInChunk) {
      if (!a.URL_PHOTO1 || !a.URL_PHOTO1.trim()) {
        skippedNoPhoto.push({ ref: a.REF_JACTAL, reason: 'Pas d\'URL photo' });
      } else {
        articlesWithPhoto.push(a);
      }
    }

    // Télécharger les images en parallèle avec concurrence limitée
    const images = []; // { filename, buffer } ou { ref, reason }
    const failed = [];
    await runWithConcurrency(articlesWithPhoto, DOWNLOAD_CONCURRENCY, async (article) => {
      try {
        const imgUrl = article.URL_PHOTO1;
        // Sécurité : vérifier le host
        const parsedUrl = new URL(imgUrl);
        if (!ALLOWED_IMAGE_HOSTS.includes(parsedUrl.host)) {
          failed.push({ ref: article.REF_JACTAL, url: imgUrl, reason: `Host non autorisé: ${parsedUrl.host}` });
          return;
        }

        const { buffer, contentType } = await downloadImage(imgUrl);
        const ext = getExtension(imgUrl, contentType);
        const baseName = buildFilename(article, rename, imgUrl);
        const filename = `${sanitizeFilename(baseName)}${ext}`;
        images.push({ filename, buffer });
      } catch (err) {
        failed.push({ ref: article.REF_JACTAL, url: article.URL_PHOTO1, reason: err.message || 'Erreur téléchargement' });
      }
    });

    // Construire le ZIP en streaming dans la réponse
    const archive = archiver('zip', { zlib: { level: 6 } });
    const chunks = [];
    archive.on('data', c => chunks.push(c));
    const archivePromise = new Promise((resolve, reject) => {
      archive.on('end', resolve);
      archive.on('error', reject);
    });

    // Ajouter les images
    const usedNames = new Set();
    for (const img of images) {
      let name = img.filename;
      let n = 1;
      // Eviter les collisions de noms
      while (usedNames.has(name)) {
        const dot = img.filename.lastIndexOf('.');
        const base = dot >= 0 ? img.filename.slice(0, dot) : img.filename;
        const ext = dot >= 0 ? img.filename.slice(dot) : '';
        name = `${base}_${n}${ext}`;
        n++;
      }
      usedNames.add(name);
      archive.append(img.buffer, { name });
    }

    // Ajouter le rapport des échecs DANS ce chunk
    const allSkipped = [
      ...skippedNoPhoto.map(s => `${s.ref} — ${s.reason}`),
      ...failed.map(s => `${s.ref} — ${s.reason} (${s.url || ''})`),
    ];

    await archive.finalize();
    await archivePromise;

    const zipBuffer = Buffer.concat(chunks);

    // En-têtes : infos sur le chunk + compteur de skipped
    const now = new Date();
    const filename = `photos_part${chunkIndex + 1}_${now.toISOString().slice(0, 10).replace(/-/g, '')}.zip`;

    // On encode le rapport des manquantes en base64 dans un header custom
    // (permet au frontend de collecter tous les rapports de tous les chunks)
    const reportPayload = {
      chunkIndex,
      totalChunks,
      totalArticles,
      downloaded: images.length,
      skipped: allSkipped,
      notFound: context.notFound || []
    };
    const reportB64 = Buffer.from(JSON.stringify(reportPayload), 'utf8').toString('base64');

    context.res = {
      status: 200,
      headers: {
        'Content-Type': 'application/zip',
        'Content-Disposition': `attachment; filename="${filename}"`,
        'X-Export-Report': reportB64,
        'X-Total-Chunks': String(totalChunks),
        'X-Total-Articles': String(totalArticles),
        'Access-Control-Expose-Headers': 'X-Export-Report, X-Total-Chunks, X-Total-Articles, Content-Disposition'
      },
      body: zipBuffer,
      isRaw: true
    };
  } catch (err) {
    context.log.error('images-zip error:', err);
    context.res = { status: 500, body: { error: err.message } };
  } finally {
    if (connection) await connection.end();
  }
};

// ===== Helpers =====

function parseList(str) {
  if (!str) return [];
  return str.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

// Télécharge une image en suivant les redirections, timeout 15s
function downloadImage(urlStr, redirectsLeft = 3) {
  return new Promise((resolve, reject) => {
    let parsed;
    try { parsed = new URL(urlStr); } catch (e) { return reject(new Error('URL invalide')); }
    const lib = parsed.protocol === 'https:' ? https : http;

    const req = lib.get(urlStr, { timeout: 15000 }, (res) => {
      // Redirection
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirectsLeft > 0) {
        res.resume();
        const next = new URL(res.headers.location, urlStr).toString();
        return downloadImage(next, redirectsLeft - 1).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const contentType = res.headers['content-type'] || '';
      const bufs = [];
      let total = 0;
      const MAX_SIZE = 20 * 1024 * 1024; // 20 Mo max par image
      res.on('data', chunk => {
        total += chunk.length;
        if (total > MAX_SIZE) {
          req.destroy();
          return reject(new Error('Image trop volumineuse (> 20 Mo)'));
        }
        bufs.push(chunk);
      });
      res.on('end', () => resolve({ buffer: Buffer.concat(bufs), contentType }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Timeout'));
    });
  });
}

function getExtension(url, contentType) {
  // 1. Essayer l'extension de l'URL
  try {
    const u = new URL(url);
    const path = u.pathname.toLowerCase();
    const m = path.match(/\.(jpe?g|png|gif|webp|bmp|tiff?)(?:$|\?)/);
    if (m) return '.' + m[1].replace('jpeg', 'jpg');
  } catch (_) {}
  // 2. Essayer content-type
  if (contentType) {
    const ct = contentType.toLowerCase();
    if (ct.includes('jpeg') || ct.includes('jpg')) return '.jpg';
    if (ct.includes('png')) return '.png';
    if (ct.includes('gif')) return '.gif';
    if (ct.includes('webp')) return '.webp';
    if (ct.includes('bmp')) return '.bmp';
    if (ct.includes('tiff')) return '.tiff';
  }
  return '.jpg'; // fallback raisonnable
}

function buildFilename(article, rename, imgUrl) {
  if (rename === 'origin') {
    // Nom tel qu'il est sur le serveur, sans extension
    try {
      const pathname = new URL(imgUrl).pathname;
      const base = pathname.split('/').pop() || 'photo';
      // Retirer l'extension (on la remettra proprement après)
      return base.replace(/\.[^.]+$/, '');
    } catch (_) {
      return 'photo';
    }
  }
  if (rename === 'ean') {
    return article.EAN_USINE || article.REF_JACTAL || 'sans_ean';
  }
  // rename === 'ref' (par défaut)
  return article.REF_JACTAL || 'sans_ref';
}

function sanitizeFilename(name) {
  // Caractères interdits sur Windows + nettoyage
  return String(name)
    .replace(/[<>:"/\\|?*\x00-\x1f]/g, '_')
    .replace(/\s+/g, '_')
    .slice(0, 200); // max 200 chars pour éviter les soucis
}

// Exécute des tâches async avec un parallélisme limité
async function runWithConcurrency(items, concurrency, worker) {
  const queue = items.slice();
  const runners = [];
  for (let i = 0; i < Math.min(concurrency, queue.length); i++) {
    runners.push((async () => {
      while (queue.length) {
        const item = queue.shift();
        await worker(item);
      }
    })());
  }
  await Promise.all(runners);
}

function buildWhere(search, f) {
  const conditions = [];
  const params = [];

  if (search) {
    conditions.push(`(REF_JACTAL LIKE ? OR NOM_FOURNISSEUR LIKE ? OR EAN_JACTAL LIKE ? OR EAN_USINE LIKE ? OR LIBELLE_STANDART LIKE ? OR LIBELLE_WEB LIKE ? OR DESCRIPTIF_WEB LIKE ? OR LICENCE_NOM LIKE ? OR MARQUE_NOM LIKE ? OR TRAD_GROUPE_NOM LIKE ? OR TRAD_SOUS_GROUPE_NOM LIKE ? OR WEB_GROUPE1_NOM LIKE ? OR WEB_SOUS_GROUPE1_NOM LIKE ?)`);
    const like = `%${search}%`;
    for (let i = 0; i < 13; i++) params.push(like);
  }
  if (f.fournisseurs.length > 0) {
    conditions.push(`NOM_FOURNISSEUR IN (${f.fournisseurs.map(() => '?').join(',')})`);
    params.push(...f.fournisseurs);
  }
  if (f.marques.length > 0) {
    conditions.push(`MARQUE_NOM IN (${f.marques.map(() => '?').join(',')})`);
    params.push(...f.marques);
  }
  if (f.licences.length > 0) {
    conditions.push(`LICENCE_NOM IN (${f.licences.map(() => '?').join(',')})`);
    params.push(...f.licences);
  }
  if (f.actif === '1' || f.actif === '0') {
    conditions.push(`ACTUEL = ?`);
    params.push(f.actif === '1' ? 1 : 0);
  }
  if (f.statuts.length > 0) {
    conditions.push(`STATUT IN (${f.statuts.map(() => '?').join(',')})`);
    params.push(...f.statuts);
  }
  if (f.cat_trad.length > 0) {
    conditions.push(`TRAD_GROUPE_NOM IN (${f.cat_trad.map(() => '?').join(',')})`);
    params.push(...f.cat_trad);
  }
  if (f.sous_cat_trad.length > 0) {
    conditions.push(`TRAD_SOUS_GROUPE_NOM IN (${f.sous_cat_trad.map(() => '?').join(',')})`);
    params.push(...f.sous_cat_trad);
  }
  if (f.cat_web.length > 0) {
    conditions.push(`WEB_GROUPE1_NOM IN (${f.cat_web.map(() => '?').join(',')})`);
    params.push(...f.cat_web);
  }
  if (f.sous_cat_web.length > 0) {
    conditions.push(`WEB_SOUS_GROUPE1_NOM IN (${f.sous_cat_web.map(() => '?').join(',')})`);
    params.push(...f.sous_cat_web);
  }
  if (f.acheteurs.length > 0) {
    conditions.push(`NOM_ACHETEUR IN (${f.acheteurs.map(() => '?').join(',')})`);
    params.push(...f.acheteurs);
  }

  const validOps = ['>', '>=', '<', '<=', '=', '!='];
  if (f.stock1_op && validOps.includes(f.stock1_op) && f.stock1_val !== '' && f.stock1_val !== undefined) {
    conditions.push(`STOCK1 ${f.stock1_op} ?`);
    params.push(Number(f.stock1_val));
  }
  if (f.stock2_op && validOps.includes(f.stock2_op) && f.stock2_val !== '' && f.stock2_val !== undefined) {
    conditions.push(`STOCK2 ${f.stock2_op} ?`);
    params.push(Number(f.stock2_val));
  }
  if (f.stock3_op && validOps.includes(f.stock3_op) && f.stock3_val !== '' && f.stock3_val !== undefined) {
    conditions.push(`STOCK3 ${f.stock3_op} ?`);
    params.push(Number(f.stock3_val));
  }

  // Filtre rayon (sous-requête sur FICDRAY)
  if (f.rayons && f.rayons.length > 0) {
    const placeholders = f.rayons.map(() => '?').join(',');
    conditions.push(`REF_JACTAL IN (
      SELECT TRIM(DR_ART) FROM FICDRAY
      WHERE CAST(LEFT(DR_NUM, 6) AS UNSIGNED) IN (${placeholders})
        AND TRIM(DR_ART) != ''
    )`);
    params.push(...f.rayons);
  }

  const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
  return { where, params };
}
