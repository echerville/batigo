import { useState } from 'react';
import Icon from './Icon.jsx';
import { deconnecter, envoyerLien, mailOk, sb } from '../lib/compte.js';

// Fenêtre « Mon compte » : connexion par lien envoyé par e-mail, sans mot de passe.
export default function Compte({ session, profil, onClose }) {
  const [email, setEmail] = useState('');
  const [etat, setEtat] = useState(null); // null | 'envoi' | 'envoye' | { erreur }
  const envoyer = async e => {
    e.preventDefault();
    if (!mailOk(email)) { setEtat({ erreur: 'Adresse e-mail invalide.' }); return; }
    setEtat('envoi');
    try { await envoyerLien(email); setEtat('envoye'); }
    catch (err) {
      const m = String(err?.message || '');
      setEtat({ erreur: /rate|limit|seconds/i.test(m) ? 'Trop de demandes : réessayez dans une minute.' : 'Envoi impossible pour le moment.' });
    }
  };
  return (
    <div className="dropdown compte glass" role="dialog" aria-label="Mon compte">
      <div className="row" style={{ justifyContent: 'space-between', marginBottom: 8 }}>
        <h2 style={{ margin: 0 }}>{session ? 'Mon compte' : 'Se connecter'}</h2>
        <button className="icon-btn" aria-label="Fermer" onClick={onClose}><Icon name="x" size={18} /></button>
      </div>
      {!sb && <p className="small muted">Les comptes ne sont pas encore disponibles.</p>}
      {sb && session && (
        <>
          <p className="small">Connecté : <strong>{session.user.email}</strong></p>
          <p className="small muted">Formule : {profil?.niveau === 'pro' ? 'Pro' : 'Gratuite'}</p>
          <button className="btn ghost" onClick={async () => { await deconnecter(); onClose(); }}>Se déconnecter</button>
        </>
      )}
      {sb && !session && etat !== 'envoye' && (
        <form onSubmit={envoyer} className="list">
          <p className="small muted" style={{ margin: 0 }}>Recevez un lien de connexion par e-mail. Déjà inscrit ? Le même lien ouvre votre compte.</p>
          <input className="rep-name" type="email" autoComplete="email" inputMode="email" placeholder="votre@adresse.fr"
            value={email} onChange={e => { setEmail(e.target.value); if (etat?.erreur) setEtat(null); }} aria-label="Adresse e-mail" />
          {etat?.erreur && <p className="small" style={{ color: 'var(--danger, #B42318)', margin: 0 }}>{etat.erreur}</p>}
          <button className="btn primary" type="submit" disabled={etat === 'envoi'}>{etat === 'envoi' ? 'Envoi…' : 'Recevoir le lien de connexion'}</button>
        </form>
      )}
      {sb && !session && etat === 'envoye' && (
        <p className="small">Lien envoyé à <strong>{email.trim()}</strong>. Ouvrez-le dans ce navigateur pour vous connecter (pensez aux indésirables).</p>
      )}
    </div>
  );
}
