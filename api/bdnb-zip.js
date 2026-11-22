// TEMPORAIRE : relais vers le fichier BDNB (département 44) pour régénérer les tuiles DPE neufs.
// Adresse figée (pas de relais ouvert), plage limitée à 4 Mo. À supprimer après usage.
const { guard } = require('./_guard');
const URL = 'https://open-data.s3.fr-par.scw.cloud/bdnb_millesime_2025-07-a/millesime_2025-07-a_dep44/open_data_millesime_2025-07-a_dep44_csv.zip';
module.exports = async (req, res) => {
  if (guard(req, res)) return;
  const start = Number(req.query.start), len = Number(req.query.len);
  if (!Number.isFinite(start) || !Number.isFinite(len) || start < 0 || len <= 0 || len > 4 * 1024 * 1024) { res.status(400).json({ error: 'plage invalide' }); return; }
  const r = await fetch(URL, { headers: { Range: `bytes=${start}-${start + len - 1}` } });
  const buf = Buffer.from(await r.arrayBuffer());
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Cache-Control', 'no-store');
  res.status(r.status === 206 ? 200 : r.status).send(buf);
};
