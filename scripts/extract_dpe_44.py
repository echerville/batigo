#!/usr/bin/env python3
"""
Extrait les bâtiments avec DPE du ZIP BDNB Loire-Atlantique → GeoJSON WGS84.
- batiment_groupe.csv          → batiment_groupe_id + geom_groupe
- batiment_groupe_dpe_representatif_logement.csv → batiment_groupe_id + classe_bilan_dpe + nb_log
Join sur batiment_groupe_id.
Usage: python3 extract_dpe_44.py bdnb_dep44.zip dpe-44.geojson
"""
import sys, zipfile, json, io
import pandas as pd
from pyproj import Transformer
from shapely import wkt as shapely_wkt
from shapely.ops import transform as shapely_transform
from shapely.geometry import mapping

TRANSFORMER = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

def detect_sep(zip_ref, name):
    with zip_ref.open(name) as f:
        line = f.readline().decode('utf-8', errors='replace')
    return ';' if line.count(';') > line.count(',') else ','

def read_csv_from_zip(zip_ref, name, usecols=None, dtype=str):
    """Lit un CSV complet depuis un ZIP (pas de chunksize — évite le problème de handle fermé)."""
    sep = detect_sep(zip_ref, name)
    raw = zip_ref.open(name)  # pas de 'with' : pandas gère la lecture jusqu'au bout
    tf = io.TextIOWrapper(raw, encoding='utf-8', errors='replace')
    return pd.read_csv(tf, sep=sep, usecols=usecols, dtype=dtype, low_memory=False)

def get_columns(zip_ref, name):
    sep = detect_sep(zip_ref, name)
    with zip_ref.open(name) as f:
        header = f.readline().decode('utf-8', errors='replace').strip()
    return [c.strip('"').strip() for c in header.split(sep)]

def convert_geom(wkt_str):
    if not wkt_str or str(wkt_str).strip() in ('', 'nan', 'None'):
        return None
    try:
        geom = shapely_wkt.loads(str(wkt_str))
        bounds = geom.bounds
        # Si coordonnées > 180 → Lambert-93, convertir en WGS84
        if abs(bounds[0]) > 180 or abs(bounds[2]) > 180:
            geom = shapely_transform(TRANSFORMER.transform, geom)
        return mapping(geom)
    except Exception:
        return None

def find_csv(zip_ref, *candidates):
    names = zip_ref.namelist()
    for c in candidates:
        matches = [n for n in names if n.endswith(c)]
        if matches:
            print(f"  Trouvé: {matches[0]}", file=sys.stderr)
            return matches[0]
    print(f"  ERREUR: aucun fichier parmi {candidates}", file=sys.stderr)
    sys.exit(1)

def main(zip_path, output_path):
    print(f"Ouverture de {zip_path}...", file=sys.stderr)

    with zipfile.ZipFile(zip_path) as z:
        # 1. Trouver les deux fichiers nécessaires
        print("Recherche des CSV...", file=sys.stderr)
        geom_file = find_csv(z, 'batiment_groupe.csv')
        dpe_file  = find_csv(z,
            'batiment_groupe_dpe_representatif_logement.csv',
            'batiment_groupe_dpe_statistique_logement.csv',
            'batiment_groupe_dpe_logement.csv')

        # 2. Afficher les colonnes disponibles
        geom_cols = get_columns(z, geom_file)
        dpe_cols  = get_columns(z, dpe_file)
        print(f"Colonnes {geom_file}: {geom_cols[:8]}...", file=sys.stderr)
        print(f"Colonnes {dpe_file}: {dpe_cols[:12]}...", file=sys.stderr)

        # 3. Choisir la colonne DPE disponible
        dpe_col = next((c for c in ['classe_bilan_dpe','classe_bilan_dpe_arrete_2021',
                                     'classe_conso_energie','etiquette_dpe']
                        if c in dpe_cols), None)
        if not dpe_col:
            print(f"ERREUR: pas de colonne DPE dans {dpe_cols}", file=sys.stderr)
            sys.exit(1)
        print(f"Colonne DPE utilisée: {dpe_col}", file=sys.stderr)

        # Colonnes nb_log
        nb_log_col = next((c for c in ['nb_log','nb_logements'] if c in dpe_cols), None)

        # Colonne date DPE
        DATE_CUTOFF = '2021-07-01'
        date_col = next((c for c in ['date_etablissement_dpe', 'date_reception_dpe',
                                      'date_depot_dpe', 'annee_reception_dpe']
                         if c in dpe_cols), None)
        print(f"Colonne date DPE: {date_col or 'non trouvée (pas de filtre date)'}", file=sys.stderr)

        # 4. Charger le CSV DPE (petit, ~quelques Mo)
        dpe_usecols = ['batiment_groupe_id', dpe_col]
        if nb_log_col:
            dpe_usecols.append(nb_log_col)
        if date_col:
            dpe_usecols.append(date_col)
        print(f"Chargement du CSV DPE ({dpe_file})...", file=sys.stderr)
        df_dpe = read_csv_from_zip(z, dpe_file, usecols=dpe_usecols)
        # Filtrer uniquement les lignes avec DPE valide
        df_dpe = df_dpe[df_dpe[dpe_col].notna() & (df_dpe[dpe_col].str.strip() != '')]
        # Filtrer les DPE antérieurs à juillet 2021
        if date_col:
            before = len(df_dpe)
            df_dpe[date_col] = pd.to_datetime(df_dpe[date_col], errors='coerce')
            df_dpe = df_dpe[df_dpe[date_col] >= DATE_CUTOFF]
            print(f"  Filtre date >= {DATE_CUTOFF}: {before} → {len(df_dpe)} bâtiments", file=sys.stderr)
        df_dpe = df_dpe.set_index('batiment_groupe_id')
        print(f"  {len(df_dpe)} bâtiments avec DPE", file=sys.stderr)

        # 5. Charger la géométrie en chunks et joindre
        geom_usecols = ['batiment_groupe_id', 'geom_groupe']
        if 'geom_groupe' not in geom_cols:
            # Chercher une colonne géométrie alternative
            alt = next((c for c in geom_cols if 'geom' in c.lower()), None)
            if alt:
                geom_usecols = ['batiment_groupe_id', alt]
                print(f"Colonne géométrie alternative: {alt}", file=sys.stderr)
            else:
                print(f"ERREUR: pas de géométrie dans {geom_cols}", file=sys.stderr)
                sys.exit(1)
        geom_col_name = geom_usecols[1]

        print(f"Chargement de la géométrie ({geom_file})...", file=sys.stderr)
        # On ne lit que les 2 colonnes utiles → léger en RAM malgré la taille du fichier
        df_geom = read_csv_from_zip(z, geom_file, usecols=geom_usecols)
        print(f"  {len(df_geom)} bâtiments avec géométrie", file=sys.stderr)
        df_geom = df_geom.set_index('batiment_groupe_id')

        # Jointure géom × DPE
        print("Jointure géom × DPE...", file=sys.stderr)
        joined = df_geom.join(df_dpe, how='inner')
        print(f"  {len(joined)} bâtiments avec géom ET DPE", file=sys.stderr)

        features = []
        for bid, row in joined.iterrows():
            geom = convert_geom(row.get(geom_col_name))
            if geom is None:
                continue
            nb = 0
            if nb_log_col and str(row.get(nb_log_col, '')).replace('.', '').isdigit():
                nb = int(float(row.get(nb_log_col, 0) or 0))
            features.append({
                'type': 'Feature',
                'geometry': geom,
                'properties': {
                    'id':     str(bid),
                    'dpe':    str(row.get(dpe_col, '') or '').strip(),
                    'nb_log': nb,
                }
            })
        print(f"  {len(features)} features avec géométrie valide", file=sys.stderr)

    print(f"\nTotal: {len(features)} bâtiments avec DPE et géométrie", file=sys.stderr)
    dpe_dist = {}
    for f in features:
        d = f['properties']['dpe']
        dpe_dist[d] = dpe_dist.get(d, 0) + 1
    print(f"Distribution DPE: {dict(sorted(dpe_dist.items()))}", file=sys.stderr)

    geojson = {'type': 'FeatureCollection', 'features': features}
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"GeoJSON écrit dans {output_path}", file=sys.stderr)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python3 extract_dpe_44.py <input.zip> <output.geojson>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
