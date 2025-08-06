# Author: Thomas A. Leibundgut
# Publications.csv recreatable from https://oam.oamonitor.ch/publications/-550575139
# PricingInfo.txt from https://doi.org/10.7910/DVN/CR1MMV
# Code was created by leaning heavily on ChatGPT: https://chatgpt.com/share/68931d21-73d8-800e-9c0d-0167d3f02064

import pandas as pd
from collections import defaultdict


def group_publications_by_journal_and_year(publications_file):
    df = pd.read_csv(publications_file, sep=';', dtype=str)

    # Convert publication date to datetime and extract year
    df['Publikationsdatum'] = pd.to_datetime(df['Publikationsdatum'], format='%d.%m.%Y', errors='coerce')
    df['year'] = df['Publikationsdatum'].dt.year

    grouped_data = []

    # Group by journal name and year
    grouped = df.groupby(['Zeitschrift', 'ISSN', 'year'])

    for (journal, issn, year), group in grouped:
        entry = {
            'journal': journal,
            'issn': issn.split(',')[0].strip() if pd.notna(issn) else None,
            'year': year,
            'number_of_publications': len(group),
            'list_apc': None  # To be filled in step 2
        }
        grouped_data.append(entry)

    return grouped_data


def add_apc_prices_to_groups(grouped_data, pricing_info_file, sep='\t'):
    import re
    pricing_df = pd.read_csv(pricing_info_file, sep=sep, dtype=str, encoding='latin1')

    # Clean & convert columns
    pricing_df['APC_year'] = pd.to_numeric(pricing_df['APC_year'], errors='coerce')
    pricing_df['APC_USD'] = pd.to_numeric(pricing_df['APC_USD'], errors='coerce')

    # Normalize journal names
    def normalize_name(name):
        return re.sub(r'\s+', ' ', name.strip().lower()) if pd.notna(name) else ''

    pricing_df['normalized_journal'] = pricing_df['Journal'].map(normalize_name)

    # Build two lookup dicts: ISSN and journal name
    from collections import defaultdict

    issn_price_lookup = defaultdict(list)
    journal_price_lookup = defaultdict(list)

    for _, row in pricing_df.iterrows():
        year = row['APC_year']
        price = row['APC_USD']
        if pd.notna(year) and pd.notna(price):
            for issn_field in ['ISSN_1', 'ISSN_2']:
                issn = str(row.get(issn_field, '')).strip()
                if issn:
                    issn_price_lookup[issn].append((int(year), float(price)))

            journal = row['normalized_journal']
            if journal:
                journal_price_lookup[journal].append((int(year), float(price)))

    # Helper: Find the closest price for a given year
    def find_closest_price(entries, target_year):
        if not entries:
            return None
        return min(entries, key=lambda x: abs(x[0] - target_year))[1]

    # Main loop: Try ISSN, then fallback to journal name
    for entry in grouped_data:
        issn = entry['issn']
        year = entry['year']
        journal = entry['journal']
        apc_price = None

        if pd.notna(issn) and pd.notna(year):
            apc_price = find_closest_price(issn_price_lookup.get(issn, []), year)

        if apc_price is None and pd.notna(journal):
            norm_journal = normalize_name(journal)
            apc_price = find_closest_price(journal_price_lookup.get(norm_journal, []), year)

        entry['list_apc'] = apc_price

    return grouped_data


def add_median_apc_estimates(grouped_data, publications_file):
    # Load publication data to get publisher per journal/year
    df = pd.read_csv(publications_file, sep=';', dtype=str)
    df['Publikationsdatum'] = pd.to_datetime(df['Publikationsdatum'], format='%d.%m.%Y', errors='coerce')
    df['year'] = df['Publikationsdatum'].dt.year
    df = df[['Zeitschrift', 'year', 'Verlag']]

    # Normalize
    df['Zeitschrift'] = df['Zeitschrift'].str.strip().str.lower()

    # Build mapping from (journal, year) -> publisher
    pub_lookup = df.dropna().drop_duplicates(subset=['Zeitschrift', 'year'])
    journal_year_to_publisher = {
        (row['Zeitschrift'], row['year']): row['Verlag'] for _, row in pub_lookup.iterrows()
    }

    # Collect all known APCs by (publisher, year)
    publisher_year_apcs = defaultdict(list)
    all_apcs = []

    for entry in grouped_data:
        apc = entry['list_apc']
        journal = entry['journal'].strip().lower() if entry['journal'] else ''
        year = entry['year']
        key = (journal, year)

        publisher = journal_year_to_publisher.get(key, None)
        if apc is not None and publisher is not None:
            publisher_year_apcs[(publisher, year)].append(apc)
            all_apcs.append(apc)

    # Precompute medians
    def median(lst):
        if not lst:
            return None
        lst = sorted(lst)
        n = len(lst)
        mid = n // 2
        return (lst[mid] if n % 2 else (lst[mid - 1] + lst[mid]) / 2)

    publisher_year_medians = {
        k: median(v) for k, v in publisher_year_apcs.items()
    }
    overall_median = median(all_apcs)

    # Helper to find closest-year median for a publisher
    def find_closest_publisher_median(publisher, target_year):
        if publisher is None:
            return None
        candidates = [(year, val) for (pub, year), val in publisher_year_medians.items() if pub == publisher]
        if not candidates:
            return None
        closest = min(candidates, key=lambda x: abs(x[0] - target_year))
        return closest[1]

    # Add fields to entries
    for entry in grouped_data:
        journal = entry['journal'].strip().lower() if entry['journal'] else ''
        year = entry['year']
        key = (journal, year)
        publisher = journal_year_to_publisher.get(key, None)

        if entry['list_apc'] is not None:
            entry['original_apc'] = 1
            entry['median_apc'] = entry['list_apc']
        else:
            entry['original_apc'] = 0
            est = find_closest_publisher_median(publisher, year)
            entry['median_apc'] = est if est is not None else overall_median

    return grouped_data

def calculate_total_spending_per_year(grouped_data):
    from collections import defaultdict

    # Dictionaries to store totals
    actual_totals = defaultdict(float)  # Based on list_apc
    estimated_totals = defaultdict(float)  # Based on median_apc

    for entry in grouped_data:
        year = entry['year']
        num_pubs = entry.get('number_of_publications', 0)
        actual_apc = entry.get('list_apc')
        estimated_apc = entry.get('median_apc')

        if pd.notna(year):
            # Total from actual APCs (only if actual APC is known)
            if actual_apc is not None:
                actual_totals[year] += actual_apc * num_pubs

            # Total from estimated APCs (should always be filled)
            if estimated_apc is not None:
                estimated_totals[year] += estimated_apc * num_pubs

    # Combine into a list of dicts for easier use
    result = []
    all_years = sorted(set(actual_totals.keys()).union(estimated_totals.keys()))
    for year in all_years:
        result.append({
            'year': year,
            'actual_total': actual_totals.get(year, 0.0),
            'estimated_total': estimated_totals.get(year, 0.0)
        })

    return result


# main function, organises the script
if __name__ == '__main__':
    data_grouped = group_publications_by_journal_and_year("data/Publications.csv")
    data_combined = add_apc_prices_to_groups(data_grouped, "data/PricingInfo.txt")
    data_enhanced = add_median_apc_estimates(data_combined, "data/Publications.csv")
    data_completed = calculate_total_spending_per_year(data_enhanced)

    for row in data_completed:
        print(f"{row['year']}: actual = ${row['actual_total']:,.2f} | estimated = ${row['estimated_total']:,.2f}")
