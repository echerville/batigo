module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { bbox } = req.query;
  if (!bbox) return res.status(400).json({ error: 'bbox required' });

  try {
    const url = `https://panoramax.ign.fr/api/search?bbox=${encodeURIComponent(bbox)}&limit=1`;
    const r = await fetch(url, { headers: { Accept: 'application/geo+json,application/json' } });
    if (!r.ok) return res.status(r.status).json({ error: 'Panoramax upstream error', status: r.status });
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
