module.exports = async function (context, req) {
  try {
    const response = await fetch('https://api.ipify.org?format=json');
    const data = await response.json();
    
    context.res = {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        azure_outbound_ip: data.ip,
        timestamp: new Date().toISOString()
      }
    };
  } catch (err) {
    context.res = {
      status: 500,
      body: { error: err.message }
    };
  }
};
