module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { bbox } = req.query;
  if (!bbox) return res.status(400).json({ error: 'bbox required' });

  try {
    const url = `https://api.bdnb.io/v1/batiment_groupe?bbox=${bbox}&limit=3&select=annee_construction,surface_shon_sum,type_energie_chauffage,type_energie_ecs,nb_logement_open,usage_type_r`;
    const response = await fetch(url);
    if (!response.ok) return res.status(response.status).json({ error: 'BDNB error' });
    const data = await response.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
