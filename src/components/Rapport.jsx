import { createPortal } from 'react-dom';
import { DPE_HEX, DPE_TEXT } from '../lib/config.js';
import {
  IMMEUBLE, bdnbEnergie, bestGrade, frDate, installType, preCoeffGrade, referenceDPE, shortEnergie,
  splitAddress, summary, usageLabel, zoneClimatique,
} from '../lib/dpe.js';

// Rapport imprimable de la fiche du bien (export PDF par l'impression du navigateur).
// Invisible à l'écran, seul visible à l'impression.
const eur = v => (v == null ? '—' : `${Math.round(Number(v)).toLocaleString('fr-FR')} €`);
const val = v => (v == null || v === '' ? '—' : v);

function Lettre({ g, from }) {
  if (!g) return <span className="rp-lettre rp-vide">—</span>;
  return (
    <span className="rp-lettre-wrap">
      {from && from !== g && <><span className="rp-lettre rp-petite" style={{ background: DPE_HEX[from], color: DPE_TEXT[from] }}>{from}</span><span className="rp-fleche">→</span></>}
      <span className="rp-lettre" style={{ background: DPE_HEX[g], color: DPE_TEXT[g] }}>{g}</span>
    </span>
  );
}

function Ligne({ k, v }) { return <tr><th>{k}</th><td>{val(v)}</td></tr>; }

export default function Rapport({ data }) {
  if (!data) return null;
  const { place, b, dpe, plu, rnc, dvf, alt, carte, reperes, quand } = data;
  const bdnb = b?.bdnb;
  const label = (place.fromClick && bdnb?.libelle_adr_principale_ban) || place.label;
  const [l1, l2] = splitAddress(label);
  const ex = dpe?.existants || [], ne = dpe?.neufs || [];
  const sum = ex.length || ne.length ? summary(ex, ne) : null;
  const grade = bestGrade(ex, ne), ref = referenceDPE(ex, ne);
  const imm = ex.filter(r => r.methode_application_dpe === IMMEUBLE);
  const logs = ex.filter(r => r.methode_application_dpe !== IMMEUBLE);
  const cad = b?.cadastre;
  const copro = rnc?.copro;
  const zone = zoneClimatique(place.citycode || bdnb?.code_commune_insee);
  const dpeLigne = (r, neuf) => (
    <tr key={r.numero_dpe}>
      <td><Lettre g={r.etiquette_dpe} from={preCoeffGrade(r)} />{neuf ? ' neuf' : ''}</td>
      <td>{r.etiquette_ges || '—'}</td>
      <td>{[r.complement_adresse_logement, r.surface_habitable_logement && `${Math.round(r.surface_habitable_logement)} m²`].filter(Boolean).join(' · ') || 'Logement'}</td>
      <td>{[shortEnergie(r.type_energie_principale_chauffage), installType(r.type_installation_chauffage_n1)].filter(Boolean).join(' ') || '—'}</td>
      <td>{frDate(r.date_reception_dpe)}</td>
      <td className="rp-num">{r.numero_dpe}</td>
    </tr>
  );

  return createPortal(
    <div className="rapport" aria-hidden="true">
      <header className="rp-tete">
        <div>
          <div className="rp-marque">Scope</div>
          <h1>{l1 || label}</h1>
          {l2 && <div className="rp-sous">{l2}</div>}
        </div>
        <div className="rp-date">Fiche du bien<br />{quand}</div>
      </header>

      {carte && <img className="rp-carte" src={carte} alt="" />}

      <section>
        <h2>Bâtiment</h2>
        <div className="rp-dpe">
          <Lettre g={grade} />
          <div>
            {ref ? <>DPE du {frDate(ref.date_reception_dpe)}{ref.conso_5_usages_par_m2_ep ? ` · ${Math.round(ref.conso_5_usages_par_m2_ep)} kWh/m²/an` : ''}{ref.etiquette_ges ? ` · GES ${ref.etiquette_ges}` : ''}</> : 'Aucun DPE publié par l’ADEME pour ce bâtiment.'}
            {ex.length + ne.length > 1 && <div className="rp-petit">{ex.length + ne.length} DPE rattachés au bâtiment</div>}
          </div>
        </div>
        <table className="rp-table rp-cles"><tbody>
          <Ligne k="Usage" v={usageLabel(bdnb?.usage_principal_bdnb_open)} />
          <Ligne k="Construction" v={bdnb?.annee_construction || sum?.year} />
          <Ligne k="Emprise au sol" v={bdnb?.surface_emprise_sol ? `${Math.round(bdnb.surface_emprise_sol)} m²` : null} />
          <Ligne k="Logements" v={bdnb?.nb_log} />
          <Ligne k="Chauffage" v={sum?.chauffage ? shortEnergie(sum.chauffage) : bdnbEnergie(bdnb?.type_energie_chauffage)} />
          <Ligne k="Eau chaude" v={sum?.ecs ? shortEnergie(sum.ecs) : bdnbEnergie(bdnb?.type_generateur_ecs)} />
          <Ligne k="Zone climatique" v={zone} />
          <Ligne k="Altitude" v={alt != null ? `${Math.round(alt)} m` : null} />
        </tbody></table>
        {!bdnb && <p className="rp-petit">Aucun bâtiment n’est rattaché de façon certaine à cette adresse.</p>}
      </section>

      {(imm.length + logs.length + ne.length) > 0 && (
        <section>
          <h2>DPE ({imm.length + logs.length + ne.length})</h2>
          <table className="rp-table">
            <thead><tr><th>DPE</th><th>GES</th><th>Logement</th><th>Chauffage</th><th>Date</th><th>N° ADEME</th></tr></thead>
            <tbody>
              {imm.map(r => dpeLigne({ ...r, complement_adresse_logement: 'Immeuble' }))}
              {logs.map(r => dpeLigne(r))}
              {ne.map(r => dpeLigne(r, true))}
            </tbody>
          </table>
        </section>
      )}

      <section>
        <h2>Parcelle et urbanisme</h2>
        <table className="rp-table rp-cles"><tbody>
          <Ligne k="Référence cadastrale" v={cad?.idu} />
          <Ligne k="Section / numéro" v={cad?.section ? `${cad.section} ${cad.numero || ''}` : null} />
          <Ligne k="Contenance" v={cad?.contenance ? `${Number(cad.contenance).toLocaleString('fr-FR')} m²` : null} />
          <Ligne k="Zone du PLU" v={plu ? [plu.libelle, plu.libelong?.replace(/^[^:]{1,16}:\s*/, '')].filter(Boolean).join(' — ') : null} />
          <Ligne k="Copropriété" v={copro ? `${copro.nom_usage_copropriete || '—'} · ${copro.nombre_total_lots || '?'} lots · syndic : ${copro.raison_sociale_representant_legal || '—'}` : null} />
        </tbody></table>
      </section>

      {Array.isArray(dvf) && dvf.length > 0 && (
        <section>
          <h2>Ventes sur la parcelle (DVF)</h2>
          <table className="rp-table">
            <thead><tr><th>Date</th><th>Prix</th><th>Biens</th><th>Surface</th><th>Pièces</th></tr></thead>
            <tbody>{dvf.map((m, i) => (
              <tr key={i}><td>{frDate(m.date_mutation)}</td><td>{eur(m.valeur_fonciere)}</td><td>{m.types_locaux?.join(', ') || m.nature_mutation || '—'}</td>
                <td>{m.surface_bati ? `${Math.round(m.surface_bati)} m²` : '—'}</td><td>{m.nb_pieces || '—'}</td></tr>
            ))}</tbody>
          </table>
        </section>
      )}

      {reperes?.length > 0 && (
        <section>
          <h2>Mes repères</h2>
          <table className="rp-table">
            <thead><tr><th>Repère</th><th>Note</th><th>Distance</th></tr></thead>
            <tbody>{reperes.map(({ r, d }) => (
              <tr key={r.id}><td><span className="rp-pastille" style={{ background: r.color }} />{r.name || 'Repère'}{r.type === 'zone' ? ' (zone)' : ''}</td><td>{r.text || '—'}</td><td>{d === 0 ? 'ici' : `${Math.round(d)} m`}</td></tr>
            ))}</tbody>
          </table>
        </section>
      )}

      <footer className="rp-pied">
        Sources : ADEME (DPE), BDNB / CSTB (bâtiment), IGN (cadastre, altitude), Géoportail de l’urbanisme (PLU), RNIC (copropriétés), DGFiP (DVF).
        Données publiques consultées le {quand}. Document indicatif, il ne remplace pas un diagnostic.
      </footer>
    </div>,
    document.body,
  );
}
