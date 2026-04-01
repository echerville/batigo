// WGS84 → Lambert-93 (pour le bbox de requête)
function wgs84ToL93(lng, lat) {
  const e = 0.08181919084, lc = 3*Math.PI/180, n = 0.7256077650, c = 11754255.426, xs = 700000, ys = 12655612.050;
  const lngR = lng*Math.PI/180, latR = lat*Math.PI/180;
  const esin = e*Math.sin(latR);
  const L = Math.log(Math.tan(Math.PI/4+latR/2)*Math.pow((1-esin)/(1+esin),e/2));
  const R = c*Math.exp(-n*L), g = n*(lngR-lc);
  return { x: xs+R*Math.sin(g), y: ys-R*Math.cos(g) };
}

// Lambert-93 → WGS84 (pour reconvertir les géométries renvoyées)
function l93ToWgs84(x, y) {
  const e = 0.08181919084, lc = 3*Math.PI/180, n = 0.7256077650, c = 11754255.426, xs = 700000, ys = 12655612.050;
  const dx = x-xs, dy = ys-y;
  const R = Math.sqrt(dx*dx+dy*dy);
  const gamma = Math.atan2(dx, dy);
  const lng = gamma/n+lc;
  const L = -Math.log(Math.abs(R/c))/n;
  let lat = 2*Math.atan(Math.exp(L))-Math.PI/2;
  for (let i=0; i<10; i++) {
    const esin = e*Math.sin(lat);
    lat = 2*Math.atan(Math.exp(L)*Math.pow((1+esin)/(1-esin),e/2))-Math.PI/2;
  }
  return [lng*180/Math.PI, lat*180/Math.PI];
}

// Conversion récursive des coordonnées d'une géométrie GeoJSON L93 → WGS84
function convertGeom(geom) {
  if (!geom) return null;
  const conv = (c) => typeof c[0]==='number' ? l93ToWgs84(c[0],c[1]) : c.map(conv);
  return { type: geom.type, coordinates: conv(geom.coordinates) };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { bbox } = req.query; // "lngMin,latMin,lngMax,latMax"
  if (!bbox) return res.status(400).json({ error: 'bbox required' });

  const [lngMin, latMin, lngMax, latMax] = bbox.split(',').map(parseFloat);
  const sw = wgs84ToL93(lngMin, latMin);
  const ne = wgs84ToL93(lngMax, latMax);

  try {
    const url = `https://api.bdnb.io/v1/bdnb/donnees/batiment_groupe_complet/bbox`
      + `?xmin=${Math.round(sw.x)}&ymin=${Math.round(sw.y)}&xmax=${Math.round(ne.x)}&ymax=${Math.round(ne.y)}`
      + `&limit=300&select=classe_bilan_dpe,geom_groupe,batiment_groupe_id,nb_log,usage_principal_bdnb_open`;

    const r = await fetch(url);
    if (!r.ok) return res.status(r.status).json({ error: 'BDNB error', status: r.status });

    const data = await r.json();
    if (!Array.isArray(data)) return res.json({ type:'FeatureCollection', features:[] });

    const features = data
      .filter(b => b.geom_groupe)
      .map(b => ({
        type: 'Feature',
        geometry: convertGeom(b.geom_groupe),
        properties: {
          id:    b.batiment_groupe_id || '',
          dpe:   b.classe_bilan_dpe  || '',
          nb_log: b.nb_log           || 0,
          usage: b.usage_principal_bdnb_open || ''
        }
      }));

    res.json({ type:'FeatureCollection', features });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
