const { guard } = require('./_guard');

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
  if (guard(req, res)) return;

  const { bbox } = req.query;
  if (!bbox) return res.status(400).json({ error: 'bbox required' });

  // bbox format: "lng_min,lat_min,lng_max,lat_max"
  const parts = String(bbox).split(',').map(Number);
  const [lngMin, latMin, lngMax, latMax] = parts;
  // Emprise valide, en France métropolitaine, et petite (l'interface demande au plus ~0,02°)
  const ok = parts.length === 4 && parts.every(Number.isFinite)
    && lngMin < lngMax && latMin < latMax
    && lngMin >= -5.5 && lngMax <= 10 && latMin >= 41 && latMax <= 51.5
    && (lngMax - lngMin) <= 0.025 && (latMax - latMin) <= 0.025;
  if (!ok) return res.status(400).json({ error: 'bbox invalide ou trop grande' });

  const sw = wgs84ToL93(lngMin, latMin);
  const ne = wgs84ToL93(lngMax, latMax);

  try {
    const url = `https://api.bdnb.io/v1/bdnb/donnees/batiment_groupe_complet/bbox?xmin=${Math.round(sw.x)}&ymin=${Math.round(sw.y)}&xmax=${Math.round(ne.x)}&ymax=${Math.round(ne.y)}&limit=20&select=annee_construction,surface_emprise_sol,type_energie_chauffage,type_generateur_ecs,nb_log,usage_principal_bdnb_open,cle_interop_adr_principale_ban,libelle_adr_principale_ban,l_parcelle_id,l_cle_interop_adr,code_iris,batiment_groupe_id`;
    const response = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!response.ok) {
      const text = await response.text();
      return res.status(response.status).json({ error: 'BDNB error', detail: text });
    }
    const data = await response.json();
    // Données publiques qui changent rarement : cache CDN Vercel 1 jour
    res.setHeader('Cache-Control', 'public, s-maxage=86400, stale-while-revalidate=86400');
    res.json(Array.isArray(data) ? data : []);
  } catch (e) {
    res.status(502).json({ error: 'Source BDNB indisponible' });
  }
};
