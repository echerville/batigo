const { guard } = require('./_guard');
const { quota, compter } = require('./_quota');

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

// Point dans un polygone (algorithme du rayon), coordonnées Lambert-93
function inRing(ring, x, y) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i], [xj, yj] = ring[j];
    if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) inside = !inside;
  }
  return inside;
}
function geomContains(geom, x, y) {
  if (!geom || !Array.isArray(geom.coordinates)) return false;
  const polys = geom.type === 'Polygon' ? [geom.coordinates] : geom.type === 'MultiPolygon' ? geom.coordinates : [];
  // Dans le contour extérieur et hors des trous éventuels
  return polys.some(([outer, ...holes]) => outer && inRing(outer, x, y) && !holes.some(h => inRing(h, x, y)));
}

const SELECT = 'annee_construction,surface_emprise_sol,type_energie_chauffage,type_generateur_ecs,nb_log,usage_principal_bdnb_open,cle_interop_adr_principale_ban,libelle_adr_principale_ban,l_parcelle_id,l_cle_interop_adr,code_iris,batiment_groupe_id';

// Mode « point » (clic sur la carte) : renvoie uniquement le ou les bâtiments
// dont le contour contient le point cliqué.
async function byPoint(point, req, res) {
  const [lng, lat] = String(point).split(',').map(Number);
  const ok = Number.isFinite(lng) && Number.isFinite(lat) && lng >= -5.5 && lng <= 10 && lat >= 41 && lat <= 51.5;
  if (!ok) return res.status(400).json({ error: 'point invalide' });
  const p = wgs84ToL93(lng, lat);
  const r = 5; // mètres autour du point
  const url = `https://api.bdnb.io/v1/bdnb/donnees/batiment_groupe_complet/bbox?xmin=${Math.floor(p.x - r)}&ymin=${Math.floor(p.y - r)}&xmax=${Math.ceil(p.x + r)}&ymax=${Math.ceil(p.y + r)}&limit=20&select=${SELECT},geom_groupe`;
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!response.ok) return res.status(502).json({ error: 'Source BDNB indisponible' });
    const data = await response.json();
    const inside = (Array.isArray(data) ? data : [])
      .filter(b => geomContains(b.geom_groupe, p.x, p.y))
      .map(({ geom_groupe, ...rest }) => rest);
        return res.json(inside);
  } catch (e) {
    return res.status(502).json({ error: 'Source BDNB indisponible' });
  }
}

module.exports = async function handler(req, res) {
  if (guard(req, res)) return;
  if (await quota(req, res)) return;

  if (req.query.point) return byPoint(req.query.point, req, res);

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
    const url = `https://api.bdnb.io/v1/bdnb/donnees/batiment_groupe_complet/bbox?xmin=${Math.round(sw.x)}&ymin=${Math.round(sw.y)}&xmax=${Math.round(ne.x)}&ymax=${Math.round(ne.y)}&limit=20&select=${SELECT}`;
    const response = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!response.ok) {
      const text = await response.text();
      return res.status(response.status).json({ error: 'BDNB error', detail: text });
    }
    const data = await response.json();
    const list = Array.isArray(data) ? data : [];
    // La fiche compte seulement si le bâtiment de l'adresse (ou du clic) est dans la réponse
    const f = String(req.query.fiche || '');
    if (list.some(b => b.batiment_groupe_id === f || b.cle_interop_adr_principale_ban === f
      || (Array.isArray(b.l_cle_interop_adr) && b.l_cle_interop_adr.includes(f)))) await compter(req);
    res.json(list);
  } catch (e) {
    res.status(502).json({ error: 'Source BDNB indisponible' });
  }
};
