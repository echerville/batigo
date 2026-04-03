// DVF géolocalisées — fichiers CSV par commune, filtrés par id_parcelle
// Source : https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/communes/{dep}/{commune}.csv
// Aucune authentification requise.

function parseCsvLine(line) {
  const cols = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { inQ = !inQ; }
    else if (c === ',' && !inQ) { cols.push(cur); cur = ''; }
    else { cur += c; }
  }
  cols.push(cur);
  return cols;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { idpar } = req.query;
  if (!idpar || idpar.length < 5) return res.status(400).json({ error: 'idpar required (14 chars)' });

  const codeCommune = idpar.slice(0, 5);
  const dep = codeCommune.startsWith('97') ? codeCommune.slice(0, 3) : codeCommune.slice(0, 2);

  const years = ['2024', '2023', '2022', '2021', '2020'];
  const allResults = [];

  await Promise.all(years.map(async year => {
    try {
      const url = `https://files.data.gouv.fr/geo-dvf/latest/csv/${year}/communes/${dep}/${codeCommune}.csv`;
      const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
      if (!r.ok) return;
      const text = await r.text();
      const lines = text.split('\n');
      if (lines.length < 2) return;

      const headers = parseCsvLine(lines[0]);
      const idParIdx = headers.indexOf('id_parcelle');
      if (idParIdx === -1) return;

      // Grouper les lignes par id_mutation pour consolider les lots
      const mutMap = {};
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        const cols = parseCsvLine(line);
        if (cols[idParIdx] !== idpar) continue;
        const obj = {};
        headers.forEach((h, j) => { obj[h] = cols[j] || ''; });
        const mid = obj.id_mutation;
        if (!mutMap[mid]) {
          mutMap[mid] = { ...obj, _rows: [obj] };
        } else {
          mutMap[mid]._rows.push(obj);
        }
      }

      // Pour chaque mutation, consolider : garder la valeur foncière unique,
      // lister les types de locaux vendus
      for (const mut of Object.values(mutMap)) {
        const rows = mut._rows;
        const types = [...new Set(rows.map(r => r.type_local).filter(Boolean))];
        const surfaces = rows.map(r => parseFloat(r.surface_reelle_bati)).filter(s => s > 0);
        const surfTotal = surfaces.reduce((a, b) => a + b, 0);
        const pieces = rows.map(r => parseInt(r.nombre_pieces_principales)).filter(n => n > 0);
        allResults.push({
          id_mutation: mut.id_mutation,
          date_mutation: mut.date_mutation,
          nature_mutation: mut.nature_mutation,
          valeur_fonciere: parseFloat(mut.valeur_fonciere) || 0,
          adresse: [mut.adresse_numero, mut.adresse_suffixe, mut.adresse_nom_voie].filter(Boolean).join(' '),
          types_locaux: types,
          surface_bati: surfTotal || null,
          nb_pieces: pieces.length ? Math.max(...pieces) : null,
          nombre_lots: parseInt(mut.nombre_lots) || rows.length,
          longitude: parseFloat(mut.longitude) || null,
          latitude: parseFloat(mut.latitude) || null,
        });
      }
    } catch (e) { /* timeout ou erreur réseau : ignorer */ }
  }));

  // Trier par date décroissante
  allResults.sort((a, b) => b.date_mutation.localeCompare(a.date_mutation));
  res.json(allResults);
};
