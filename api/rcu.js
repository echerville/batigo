// Proxy pour l'API France Chaleur Urbaine (CORS bloqué en direct)
// Filtre les réseaux de chaleur sur la bbox Loire-Atlantique
const BOUNDS = { minLng: -2.55, maxLng: -0.85, minLat: 46.85, maxLat: 47.95 };

function inLoire(geom) {
  for (const ls of (geom?.coordinates || [])) {
    for (const pt of ls) {
      const [x, y] = Array.isArray(pt[0]) ? pt[0] : pt;
      if (x > BOUNDS.minLng && x < BOUNDS.maxLng && y > BOUNDS.minLat && y < BOUNDS.maxLat) return true;
    }
  }
  return false;
}

export default async function handler(req, res) {
  if (req.method !== 'GET') return res.status(405).end();
  try {
    const r = await fetch('https://france-chaleur-urbaine.beta.gouv.fr/api/v1/networks', {
      headers: { 'User-Agent': 'scope-app/1.0' }
    });
    if (!r.ok) return res.status(r.status).json({ error: 'FCU error' });
    const data = await r.json();
    const features = data
      .filter(n => n.geom && inLoire(n.geom))
      .map(n => ({
        type: 'Feature',
        geometry: n.geom,
        properties: {
          nom: n['nom_reseau'] || '',
          gestionnaire: n['Gestionnaire'] || '',
          enr: n['Taux EnR&R'] != null ? n['Taux EnR&R'] + ' %' : '—',
          co2: n['contenu CO2'] != null ? n['contenu CO2'] + ' kg/kWh' : '—'
        }
      }));
    res.setHeader('Cache-Control', 's-maxage=86400, stale-while-revalidate');
    res.status(200).json({ type: 'FeatureCollection', features });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}
