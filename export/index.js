const mysql = require('mysql2');
const ExcelJS = require('exceljs');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

// Ordre officiel des colonnes dans la VIEW V_ARTICLES_CATALOGUE
const VIEW_ORDER = [
  'REF_JACTAL','ACTUEL','CODE_FOURNISSEUR','NOM_FOURNISSEUR','REF_ART_FOURNISSEUR','EAN_USINE','EAN_JACTAL',
  'COMMENTAIRE_INTERNE','PA','DEVISE','PR','CODE_VALORLUX','LIBELLE_STANDART','LIBELLE_WEB','DESCRIPTIF_WEB',
  'AGE_PREVU_TW','MARQUE_TW','STOCK1','STOCK2','STOCK3','HT1_CFT','HT1_PRIX','HT2_PRIX','HT3_CFT','HT3_PRIX',
  'HT4_CFT','HT4_PRIX','HT5_CFT','HT5_PRIX','HT6_CFT','HT6_PRIX','TTC1_PRIX','TTC2_PRIX','TTC3_PRIX','TTC4_PRIX',
  'TTC5_PRIX','TTC6_PRIX','REGROUPE','GROUPE_REMISE','COLIS_INT','COLIS_EXT','MIN_CMDE','WEB_PVTTC_PROPO',
  'WEB1_CFT','WEB_HT1_PRIX','WEB1_QTE','WEB2_CFT','WEB_HT2_PRIX','WEB2_QTE','WEB3_CFT','WEB_HT3_PRIX','WEB3_QTE',
  'WEB4_CFT','WEB4_PRIX','WEB4_QTE','PRIX_GROSSISTE','QTE_GROSSISTE','REMISE_TEMPORAIRE','TRAD_GROUPE',
  'TRAD_GROUPE_NOM','TRAD_SOUS_GROUPE','TRAD_SOUS_GROUPE_NOM','CODE_DOUANIER','LARGEUR_MM_PRODUIT',
  'HAUTEUR_MM_PRODUIT','PROFONDEUR_MM_PRODUIT','POIDS_G_PRODUIT','LARGEUR_MM_COLIS','HAUTEUR_MM_COLIS',
  'PROFONDEUR_MM_COLIS','POIDS_G_COLIS','NBRE_COLIS_COUCHE','EUROPALETE_NBRE_COUCHE','NBRE_BATT_LR03',
  'NBRE_BATT_LR06','NBRE_BATT_LR14','NBRE_BATT_LR20','NBRE_BATT_9V','NBRE_BATT_AKKU','ARTICLE_WEB',
  'WEB_GROUPE1','WEB_GROUPE1_NOM','WEB_SOUS_GROUPE1','WEB_SOUS_GROUPE1_NOM','WEB_GROUPE2','WEB_GROUPE2_NOM',
  'WEB_SOUS_GROUPE2','WEB_SOUS_GROUPE2_NOM','WEB_GROUPE3','WEB_GROUPE3_NOM','WEB_SOUS_GROUPE3',
  'WEB_SOUS_GROUPE3_NOM','WEB_TOUS_PAYS','WEB_EXCLURE_LUX','WEB_EXCLURE_FRA','WEB_EXCLURE_BEL',
  'WEB_EXCLURE_ALL','WEB_DATE_MISE_WEB','WEB_PRODUIT_PERMANENT','WEB_PROD_ZONE_DEFIL','WEB_PROD_TETE_CAT',
  'WEB_PROD_PRECO','WEB_CMDE_AVANT','WEB_ARRIVAGE_PREVU','WEB_ST1','WEB_ST2','DATE_DEBUT_SAISON',
  'DATE_FIN_SAISON','CLE_MARQUE','MARQUE_NOM','CLE_LICENCE','LICENCE_NOM','LIMITE_COM','URL_PHOTO1',
  'STATUT','PERTINANCE','DANGEREUX','CLASSIFICATION','POINT_ECLAIR','IMDG','LITRAGE','DATE_VALIDITE_SECURE',
  'EAN_COLIS','EAN_PALETTE','DESCRIPTION_UR','DESCRIPTION_UC','QTE_UC_PAR_UR','DESCRIPTION_UV','QTE_UV_PAR_UC',
  'CODE_ACHETEUR','NOM_ACHETEUR','FICHE_SECURITE'
];
const ALLOWED_COLUMNS = new Set(VIEW_ORDER);
const MAX_CELLS = 50_000_000; // Garde-fou haut : 50 millions de cellules

module.exports = async function (context, req) {
  const clientPrincipal = req.headers['x-ms-client-principal'];
  if (!clientPrincipal) {
    context.res = { status: 401, body: { error: 'Unauthorized' } };
    return;
  }

  const search = (req.query.search || '').trim();
  const requestedColumns = parseList(req.query.columns);
  const format = (req.query.format === 'csv') ? 'csv' : 'xlsx';

  if (requestedColumns.length === 0) {
    context.res = { status: 400, body: { error: 'No columns selected' } };
    return;
  }

  const validColumns = requestedColumns.filter(c => ALLOWED_COLUMNS.has(c));
  if (validColumns.length === 0) {
    context.res = { status: 400, body: { error: 'No valid columns' } };
    return;
  }

  let finalColumns;
  if (validColumns.length === VIEW_ORDER.length) {
    finalColumns = [...VIEW_ORDER];
  } else {
    finalColumns = validColumns;
  }

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
    rayons_master: parseList(req.query.rayons_master).map(r => parseInt(r, 10)).filter(n => !isNaN(n)),
    rayons_client: parseList(req.query.rayons_client).map(r => parseInt(r, 10)).filter(n => !isNaN(n)),
    stock1_op: req.query.stock1_op, stock1_val: req.query.stock1_val,
    stock2_op: req.query.stock2_op, stock2_val: req.query.stock2_val,
    stock3_op: req.query.stock3_op, stock3_val: req.query.stock3_val,
  };

  // Fichier temporaire pour stocker le XLSX pendant sa génération streaming
  const tmpFilename = path.join(os.tmpdir(), `export_${crypto.randomBytes(8).toString('hex')}.xlsx`);
  let connection;

  try {
    // Connexion mysql2 en mode callback (non-promise) pour pouvoir utiliser query().stream()
    connection = mysql.createConnection({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE
    });

    await connectAsync(connection);

    const { where, params } = buildWhere(search, f);
    const colsList = finalColumns.map(c => `\`${c}\``).join(', ');

    // GARDE-FOU HAUT : compter les lignes avant (empêche les abus absurdes)
    const [countRows] = await queryAsync(
      connection,
      `SELECT COUNT(*) AS total FROM V_ARTICLES_CATALOGUE ${where}`,
      params
    );
    const totalRows = countRows[0].total;
    const totalCells = totalRows * finalColumns.length;

    if (totalCells > MAX_CELLS) {
      context.res = {
        status: 400,
        body: {
          error: 'EXPORT_TOO_LARGE',
          message: `Export trop volumineux : ${totalRows.toLocaleString('fr-FR')} lignes × ${finalColumns.length} colonnes = ${(totalCells / 1_000_000).toFixed(1)} millions de cellules. Limite : ${(MAX_CELLS / 1_000_000).toFixed(0)} millions.`,
          totalRows: totalRows,
          totalColumns: finalColumns.length,
          totalCells: totalCells,
          maxCells: MAX_CELLS,
          suggestedMaxRows: Math.floor(MAX_CELLS / finalColumns.length),
          suggestedMaxColumns: Math.floor(MAX_CELLS / Math.max(totalRows, 1))
        }
      };
      return;
    }

    context.log(`Export streaming démarré : ${totalRows} lignes × ${finalColumns.length} colonnes (format: ${format})`);
    const startTime = Date.now();

    if (format === 'csv') {
      // ================ BRANCHE CSV ================
      const csvPath = tmpFilename.replace(/\.xlsx$/, '.csv');
      const writeStream = fs.createWriteStream(csvPath, { encoding: 'utf8' });

      // BOM UTF-8 pour Excel (accents)
      writeStream.write('\uFEFF');

      // Header : séparateur point-virgule pour Excel FR
      writeStream.write(finalColumns.map(csvEscape).join(';') + '\n');

      await new Promise((resolve, reject) => {
        const queryStream = connection.query(
          `SELECT ${colsList} FROM V_ARTICLES_CATALOGUE ${where} ORDER BY PERTINANCE DESC, REF_JACTAL ASC`,
          params
        ).stream({ highWaterMark: 500 });

        let rowCount = 0;

        queryStream.on('error', (err) => {
          context.log.error('Erreur MySQL stream:', err);
          writeStream.end();
          reject(err);
        });

        queryStream.on('data', (row) => {
          const line = finalColumns.map(col => csvEscape(row[col])).join(';') + '\n';
          writeStream.write(line);
          rowCount++;
          if (rowCount % 20000 === 0) {
            context.log(`  ${rowCount} lignes écrites...`);
          }
        });

        queryStream.on('end', () => {
          writeStream.end(() => {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            context.log(`Export CSV terminé : ${rowCount} lignes en ${elapsed}s`);
            resolve();
          });
        });
      });

      const buffer = fs.readFileSync(csvPath);
      try { fs.unlinkSync(csvPath); } catch (_) {}
      const now = new Date();
      const filename = `articles_${now.toISOString().slice(0, 10).replace(/-/g, '')}_${now.getHours()}${String(now.getMinutes()).padStart(2, '0')}.csv`;

      context.res = {
        status: 200,
        headers: {
          'Content-Type': 'text/csv; charset=utf-8',
          'Content-Disposition': `attachment; filename="${filename}"`
        },
        body: buffer,
        isRaw: true
      };
      return;
    }

    // ================ BRANCHE XLSX (streaming) ================
    // Création du workbook en mode STREAMING (écrit dans un fichier temporaire)
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      filename: tmpFilename,
      useStyles: true,
      useSharedStrings: false // Plus rapide et moins gourmand en RAM
    });

    const sheet = workbook.addWorksheet('Articles');
    sheet.columns = finalColumns.map(c => ({ header: c, key: c, width: 20 }));

    // Header stylisé
    const headerRow = sheet.getRow(1);
    headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E3A5F' } };
    headerRow.commit();

    // Streaming MySQL → Excel (RAM constante ~50-100 Mo)
    await new Promise((resolve, reject) => {
      const queryStream = connection.query(
        `SELECT ${colsList} FROM V_ARTICLES_CATALOGUE ${where} ORDER BY PERTINANCE DESC, REF_JACTAL ASC`,
        params
      ).stream({ highWaterMark: 500 });

      let rowCount = 0;

      queryStream.on('error', (err) => {
        context.log.error('Erreur MySQL stream:', err);
        reject(err);
      });

      queryStream.on('data', (row) => {
        sheet.addRow(row).commit();
        rowCount++;
        if (rowCount % 10000 === 0) {
          context.log(`  ${rowCount} lignes écrites...`);
        }
      });

      queryStream.on('end', async () => {
        try {
          await sheet.commit();
          await workbook.commit();
          const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
          context.log(`Export XLSX terminé : ${rowCount} lignes en ${elapsed}s`);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    // Lire le fichier généré et le retourner
    const buffer = fs.readFileSync(tmpFilename);
    const now = new Date();
    const filename = `articles_${now.toISOString().slice(0, 10).replace(/-/g, '')}_${now.getHours()}${String(now.getMinutes()).padStart(2, '0')}.xlsx`;

    context.res = {
      status: 200,
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="${filename}"`
      },
      body: buffer,
      isRaw: true
    };
  } catch (err) {
    context.log.error('Export error:', err);
    context.res = { status: 500, body: { error: err.message } };
  } finally {
    // Nettoyage : fermer MySQL et supprimer le fichier temporaire
    if (connection) {
      try { connection.end(); } catch (_) {}
    }
    try { if (fs.existsSync(tmpFilename)) fs.unlinkSync(tmpFilename); } catch (_) {}
  }
};

// Helpers pour wrapper les callbacks mysql2 en promises
function connectAsync(conn) {
  return new Promise((resolve, reject) => {
    conn.connect(err => err ? reject(err) : resolve());
  });
}

function queryAsync(conn, sql, params) {
  return new Promise((resolve, reject) => {
    conn.query(sql, params, (err, results, fields) => {
      if (err) reject(err);
      else resolve([results, fields]);
    });
  });
}

function parseList(str) {
  if (!str) return [];
  return str.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

// Échappe une valeur pour CSV (RFC 4180) : entoure de guillemets si contient ; " \n ou \r
function csvEscape(val) {
  if (val === null || val === undefined) return '';
  let str = String(val);
  if (str.includes(';') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    str = '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
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

  // Filtre rayon (sous-requête sur FICDRAY) — combine master + client
  const allRayonIds = [...(f.rayons_master || []), ...(f.rayons_client || [])];
  if (allRayonIds.length > 0) {
    const placeholders = allRayonIds.map(() => '?').join(',');
    conditions.push(`REF_JACTAL IN (
      SELECT TRIM(DR_ART) FROM FICDRAY
      WHERE CAST(LEFT(DR_NUM, 6) AS UNSIGNED) IN (${placeholders})
        AND TRIM(DR_ART) != ''
    )`);
    params.push(...allRayonIds);
  }

  const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
  return { where, params };
}
