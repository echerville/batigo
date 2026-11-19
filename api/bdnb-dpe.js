// Désactivé (étape 1 sécurité, 19/11/2026) : non utilisé par l'interface et ouvert à tous.
// L'ancien code reste dans l'historique Git et dans api-avant-etape1/.
module.exports = function handler(req, res) {
  res.status(410).json({ error: "Point d'accès désactivé" });
};
