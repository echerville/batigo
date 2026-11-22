import { useState } from 'react';
import Icon from './Icon.jsx';
import { MDP_MIN, changerMdp, connecter, deconnecter, inscrire, mailOk, messageErreur, oubli, sb } from '../lib/compte.js';

const TITRES = { connexion: 'Se connecter', inscription: 'Créer un compte', oubli: 'Mot de passe oublié', nouveau: 'Nouveau mot de passe' };

// Fenêtre « Mon compte » : connexion par e-mail et mot de passe.
export default function Compte({ session, profil, recuperation, onRecupere, onClose }) {
  const [mode, setMode] = useState(recuperation ? 'nouveau' : 'connexion');
  const [email, setEmail] = useState('');
  const [mdp, setMdp] = useState('');
  const [mdp2, setMdp2] = useState('');
  const [voir, setVoir] = useState(false);
  const [etat, setEtat] = useState(null); // null | 'envoi' | { ok } | { erreur }
  const [changer, setChanger] = useState(false);

  const aller = m => { setMode(m); setEtat(null); setMdp(''); setMdp2(''); };
  const lancer = async (fn, ok) => {
    setEtat('envoi');
    try { const r = await fn(); setEtat(ok ? { ok: typeof ok === 'function' ? ok(r) : ok } : null); }
    catch (err) { setEtat({ erreur: messageErreur(err) }); }
  };
  const verifMdp = () => {
    if (mdp.length < MDP_MIN) return `Au moins ${MDP_MIN} caractères.`;
    if (mdp !== mdp2) return 'Les deux mots de passe ne correspondent pas.';
    return null;
  };
  const soumettre = e => {
    e.preventDefault();
    if (mode !== 'nouveau' && !mailOk(email)) { setEtat({ erreur: 'Adresse e-mail invalide.' }); return; }
    if (mode === 'connexion') {
      if (!mdp) { setEtat({ erreur: 'Saisissez votre mot de passe.' }); return; }
      lancer(() => connecter(email, mdp), null).then(() => {});
    } else if (mode === 'inscription') {
      const v = verifMdp(); if (v) { setEtat({ erreur: v }); return; }
      lancer(() => inscrire(email, mdp), direct => (direct ? 'Compte créé, vous êtes connecté.' : `Compte créé. Confirmez votre adresse avec le lien envoyé à ${email.trim()}, puis connectez-vous.`));
    } else if (mode === 'oubli') {
      lancer(() => oubli(email), `Si un compte existe pour ${email.trim()}, un e-mail vient d’être envoyé pour choisir un nouveau mot de passe.`);
    } else if (mode === 'nouveau') {
      const v = verifMdp(); if (v) { setEtat({ erreur: v }); return; }
      lancer(() => changerMdp(mdp), 'Mot de passe enregistré.').then(() => onRecupere?.());
    }
  };
  const changerSoumettre = e => {
    e.preventDefault();
    const v = verifMdp(); if (v) { setEtat({ erreur: v }); return; }
    lancer(() => changerMdp(mdp), 'Mot de passe enregistré.').then(() => { setMdp(''); setMdp2(''); });
  };

  const champMdp = (val, set, label, auto) => (
    <div className="mdp">
      <input className="rep-name" type={voir ? 'text' : 'password'} autoComplete={auto} placeholder={label} aria-label={label}
        value={val} onChange={e => { set(e.target.value); if (etat?.erreur) setEtat(null); }} />
      <button type="button" className="mdp-voir small" onClick={() => setVoir(v => !v)}>{voir ? 'Masquer' : 'Afficher'}</button>
    </div>
  );
  const messages = (
    <>
      {etat?.erreur && <p className="small" style={{ color: '#B42318', margin: 0 }}>{etat.erreur}</p>}
      {etat?.ok && <p className="small" style={{ margin: 0 }}>{etat.ok}</p>}
    </>
  );
  const enCompte = session && !(recuperation && mode === 'nouveau');

  return (
    <div className="dropdown compte glass" role="dialog" aria-label={enCompte ? 'Mon compte' : TITRES[mode]}>
      <div className="row" style={{ justifyContent: 'space-between', marginBottom: 8 }}>
        <h2 style={{ margin: 0 }}>{enCompte ? 'Mon compte' : TITRES[mode]}</h2>
        <button className="icon-btn" aria-label="Fermer" onClick={onClose}><Icon name="x" size={18} /></button>
      </div>
      {!sb && <p className="small muted">Les comptes ne sont pas encore disponibles.</p>}

      {sb && enCompte && (
        <div className="list">
          <p className="small" style={{ margin: 0 }}>Connecté : <strong>{session.user.email}</strong></p>
          <p className="small muted" style={{ margin: 0 }}>Formule : {profil?.niveau === 'pro' ? 'Pro' : 'Gratuite'}</p>
          {!changer && <button className="btn ghost" onClick={() => { setChanger(true); setEtat(null); }}>Définir ou changer le mot de passe</button>}
          {changer && (
            <form onSubmit={changerSoumettre} className="list">
              {champMdp(mdp, setMdp, 'Nouveau mot de passe', 'new-password')}
              {champMdp(mdp2, setMdp2, 'Confirmer le mot de passe', 'new-password')}
              {messages}
              <button className="btn primary" type="submit" disabled={etat === 'envoi'}>{etat === 'envoi' ? 'Enregistrement…' : 'Enregistrer'}</button>
            </form>
          )}
          <button className="btn ghost" onClick={async () => { await deconnecter(); onClose(); }}>Se déconnecter</button>
        </div>
      )}

      {sb && !enCompte && (
        <form onSubmit={soumettre} className="list">
          {mode !== 'nouveau' && (
            <input className="rep-name" type="email" autoComplete="email" inputMode="email" placeholder="Adresse e-mail" aria-label="Adresse e-mail"
              value={email} onChange={e => { setEmail(e.target.value); if (etat?.erreur) setEtat(null); }} />
          )}
          {mode === 'connexion' && champMdp(mdp, setMdp, 'Mot de passe', 'current-password')}
          {(mode === 'inscription' || mode === 'nouveau') && (
            <>
              {champMdp(mdp, setMdp, `Mot de passe (${MDP_MIN} caractères min.)`, 'new-password')}
              {champMdp(mdp2, setMdp2, 'Confirmer le mot de passe', 'new-password')}
            </>
          )}
          {messages}
          {!etat?.ok && (
            <button className="btn primary" type="submit" disabled={etat === 'envoi'}>
              {etat === 'envoi' ? 'Patientez…' : { connexion: 'Se connecter', inscription: 'Créer mon compte', oubli: 'Recevoir l’e-mail', nouveau: 'Enregistrer' }[mode]}
            </button>
          )}
          <div className="compte-liens small">
            {mode === 'connexion' && <><button type="button" onClick={() => aller('oubli')}>Mot de passe oublié ?</button><button type="button" onClick={() => aller('inscription')}>Créer un compte</button></>}
            {(mode === 'inscription' || mode === 'oubli') && <button type="button" onClick={() => aller('connexion')}>J’ai déjà un compte : se connecter</button>}
          </div>
        </form>
      )}
    </div>
  );
}
