module.exports = async function (context, req) {
  const results = {};
  
  // Test 1: ipify
  try {
    const r1 = await fetch('https://api.ipify.org?format=json');
    const d1 = await r1.json();
    results.ipify = d1.ip;
  } catch (e) {
    results.ipify_error = e.message;
  }
  
  // Test 2: icanhazip
  try {
    const r2 = await fetch('https://icanhazip.com');
    const txt = await r2.text();
    results.icanhazip = txt.trim();
  } catch (e) {
    results.icanhazip_error = e.message;
  }
  
  // Test 3: ifconfig.me
  try {
    const r3 = await fetch('https://ifconfig.me/ip');
    const txt = await r3.text();
    results.ifconfigme = txt.trim();
  } catch (e) {
    results.ifconfigme_error = e.message;
  }
  
  results.expected_nat_ip = '134.149.232.156';
  results.timestamp = new Date().toISOString();
  
  context.res = {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
    body: results
  };
};
