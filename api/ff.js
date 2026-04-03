module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { idpar } = req.query;
  if (!idpar) return res.status(400).json({ error: 'idpar required' });

  try {
    const url = `https://apidf-preprod.cerema.fr/ff/locaux/?idpar=${encodeURIComponent(idpar)}&page_size=100`;
    const r = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (!r.ok) return res.status(r.status).json({ error: `FF API: ${r.status}` });
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
