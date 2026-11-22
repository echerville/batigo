// Comptes Scope (Supabase, Paris). Clé publiable : prévue pour le navigateur, l'accès aux données
// est protégé par les règles de la base (chacun ne lit et n'écrit que ses propres lignes).
import { createClient } from '@supabase/supabase-js';
import { SUPABASE_URL, SUPABASE_KEY } from './config.js';

export const sb = SUPABASE_URL && SUPABASE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true } })
  : null;

const RE_MAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
export const mailOk = m => RE_MAIL.test((m || '').trim());
export const MDP_MIN = 8;
const retour = () => location.origin + location.pathname;

// Traduit les erreurs de Supabase en phrases claires
export function messageErreur(err) {
  const m = String(err?.message || err || '');
  if (/invalid login credentials/i.test(m)) return 'E-mail ou mot de passe incorrect.';
  if (/email not confirmed/i.test(m)) return 'Adresse pas encore confirmée : ouvrez le lien reçu par e-mail.';
  if (/already registered|already been registered|exists/i.test(m)) return 'Un compte existe déjà avec cette adresse : connectez-vous.';
  if (/rate|limit|seconds/i.test(m)) return 'Trop de demandes : réessayez dans une minute.';
  if (/password.*(least|short|weak)/i.test(m)) return `Mot de passe trop court (au moins ${MDP_MIN} caractères).`;
  if (/same.*password|different from the old/i.test(m)) return 'Choisissez un mot de passe différent de l’ancien.';
  return 'Opération impossible pour le moment.';
}

export async function connecter(email, password) {
  const { error } = await sb.auth.signInWithPassword({ email: email.trim(), password });
  if (error) throw error;
}
// Inscription : Supabase envoie un e-mail de confirmation (lien qui ramène sur Scope)
export async function inscrire(email, password) {
  const { data, error } = await sb.auth.signUp({ email: email.trim(), password, options: { emailRedirectTo: retour() } });
  if (error) throw error;
  // Adresse déjà inscrite : Supabase renvoie un utilisateur sans identité (par discrétion)
  if (data?.user && Array.isArray(data.user.identities) && data.user.identities.length === 0) throw new Error('already registered');
  return !!data?.session; // true si connecté directement (confirmation désactivée)
}
export async function oubli(email) {
  const { error } = await sb.auth.resetPasswordForEmail(email.trim(), { redirectTo: retour() });
  if (error) throw error;
}
export async function changerMdp(password) {
  const { error } = await sb.auth.updateUser({ password });
  if (error) throw error;
}
export async function deconnecter() { if (sb) await sb.auth.signOut(); }

// Profil : niveau (gratuit ou pro). Créé par la base à l'inscription.
export async function lireProfil(uid) {
  if (!sb || !uid) return null;
  const { data } = await sb.from('profiles').select('niveau, cree_le').eq('id', uid).maybeSingle();
  return data;
}
