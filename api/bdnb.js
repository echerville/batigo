// Conversion WGS84 (lng/lat) → Lambert-93 (EPSG:2154)
// Formule simplifiée suffisamment précise pour un bbox de quelques centaines de mètres
function wgs84ToL93(lng, lat) {
  const a = 6378137.0;
  const e = 0.08181919084;
  const lc = 3.0 * Math.PI / 180;
  const n = 0.7256077650;
  const c = 11754255.426;
  const xs = 700000.0;
  const ys = 12655612.050;

  const lngRad = lng * Math.PI / 180;
  const latRad = lat * Math.PI / 180;

  const esinlat = e * Math.sin(latRad);
  const L = Math.log(Math.tan(Math.PI / 4 + latRad / 2) * Math.pow((1 - esinlat) / (1 + esinlat), e / 2));
  const R = c * Math.exp(-n * L);
  const gamma = n * (lngRad - lc);

  const X = xs + R * Math.sin(gamma);
  const Y = ys - R * Math.cos(gamma);
  return { x: X, y: Y };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { bbox } = req.query;
  if (!bbox) return res.status(400).json({ error: 'bbox required' });

  // bbox format: "lng_min,lat_min,lng_max,lat_max"
  const parts = bbox.split(',').map(parseFloat);
  const [lngMin, latMin, lngMax, latMax] = parts;

  const sw = wgs84ToL93(lngMin, latMin);
  const ne = wgs84ToL93(lngMax, latMax);

  try {
    const url = `https://api.bdnb.io/v1/bdnb/donnees/batiment_groupe_complet/bbox?xmin=${Math.round(sw.x)}&ymin=${Math.round(sw.y)}&xmax=${Math.round(ne.x)}&ymax=${Math.round(ne.y)}&limit=20&select=annee_construction,surface_emprise_sol,type_energie_chauffage,type_generateur_ecs,nb_log,usage_principal_bdnb_open,cle_interop_adr_principale_ban,libelle_adr_principale_ban,l_parcelle_id,code_iris,batiment_groupe_id`;
    const response = await fetch(url);
    if (!response.ok) {
      const text = await response.text();
      return res.status(response.status).json({ error: 'BDNB error', detail: text });
    }
    const data = await response.json();
    res.json(Array.isArray(data) ? data : []);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
