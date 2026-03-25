# Analýza cross-chain protokolů (Bakalářská práce)

Tento repozitář obsahuje sadu skriptů, datových pipelin a analytických Jupyter notebooků vytvořených jako součást mé bakalářské práce. Cílem této práce je analýza výkonnosti, ceny a latence tří hlavních cross-chain komunikačních protokolů: **Axelar**, **IBC (Inter-Blockchain Communication)** a **XCM (Cross-Consensus Message Format)**.

Struktura repozitáře je rozdělena do 3 hlavních oddílů odpovídajících těmto třem zkoumaným technologiím.

---

## 1. Axelar (`/Axelar/`)
Tato složka je zaměřena na získávání a analýzu dat pro General Message Passing (GMP) zprostředkovaných sítí Axelar. Hlavním zdrojem dat je poskytované Axelarscan API.

### Soubory a adresáře:
- **`axelar_downloader.py`**: Skript sloužící pro stažení GMP (General Message Passing) transakcí z API Axelarscanu s ohledem na stránkovací limity. Obsahuje módy pro stahování konkrétní transakce, vyhledávání záznamů a dávkové stahování (batchování) a exportuje je primárně do CSV/JSON.
- **`build_axelar_metrics.py`**: Komplexní procesní skript, který z načtených surových CSV tvoří čistý dataset (`axelar_bridge_clean.csv`). Stará se o normalizaci sloupečků (rozkódování JSON atributů), extrakci přesných časových úseků a počítá metriky výkonnosti protokolu – např. `e2e_sec` (celková latence), odhadnuté poplatky `total_cost_usd` a agreguje finální hodnoty chybovosti (success rate) do statistik na úrovni párů sítí (`axelar_bridge_pair_summary.csv`).
- **`axelarscan_získání_dat_api-2.ipynb`**: Experimentační a průzkumný Jupyter notebook (EDA) sloužící pro předzpracování a první náhledy na posbíraná data o přenosech.
- **`notebook/`**: Další adresář s analýzami, které z dat zpracovaných primárními skripty čerpají.

---

## 2. IBC (Inter-Blockchain Communication) (`/IBC_BP/`)
Protokol IBC sloužící především k bezpečné a spolehlivé komunikaci nezávislých aplikačně specifických blockchainů (Cosmos SDK zón). 

### Soubory a adresáře:
- **`ibc_download.py`**: Ucelený skript pro získávání IBC paketů (typu `send_packet`, `recv_packet` a `acknowledge_packet`) kombinováním Cosmos directory REST, Cosmos IBC Chain Registry (github metadat) a dotazování pomocí CometBFT RPC. Po zřetězení událostí skrz různé chainy skript exportuje denormalizovanou tabulku zahrnující pakety, sekvence, síťové poplatky, i cílovou end-to-end latenci přímo do CSV (`ibc_sample.csv`).
- **`ibc.ipynb`**: Pokročilý analytický notebook, který zpracovává výstupy z IBC a připravuje datové agregace a grafy (CDF rozdělení atp.) pro text bakalářské práce.

---

## 3. XCM (Cross-Consensus Message Format) (`/XCM_BP/`)
Sekce určená k analýze formátu XCM, který primárně pohání komunikaci v ekosystému sítě Polkadot a Kusama (relay chainy) spolu s jejich připojenými parachainy. Skripty zde čerpají data ze Subscan API.

### Soubory a adresáře:
- **`pipeline.py`**: Hlavní datová pipeline extrahující modulové *extrinsics* transakce ze Subscan V2 API s ošetřením občasných Rate-Limitů (HTTP 429). Stahuje transakce relevantní pro moduly, jako jsou `polkadotXcm`, `xTokens` nebo `dmp`. Data flattenuje a následně počítá metriky jako success rate modulu a celkový počet zpráv za den. Výstup zpracovává do agregovaných souborů (do složky `/output/`).
- **`fill_costs.py`**: Skript vytvořený k prohledání již existujícího datasetu (založeném na extrinsic indexech iterativně) a doplňující dodatečné náklady (fee / cost) pro sledované operace. Pomáhá tím získat pro analýzu detailní cenovou hladinu pro XCM přenosy.
- **`corrected_script.py`**: Pomocný procesní skript pro transformace, párování obou stran přesunu (multichain data) na Polkadotu či parachainu, a opravy surových událostí.
- **Doplňková kontrolní data a json soubory**: V této složce naleznete řadu surových vzorových .json výpisů (např. `assethub_block.json`, `polkadot_block.json`, nebo ukázky `mq_processed`), které posloužily pro zpětnou analýzu událostního logu z parachainů.
- **`notebook/`** a **`output/`**: Složky hostující výsledné CSV logy (suhrny za moduly či chainy) i průzkumné notebooky (výpočty CDF, time-series grafy poplatků a podobně).

---

## Prerekvizity a Použití

Pokud se rozhodnete spouštět zkušební dávky či skripty ze sady lokálně, je ujednána níže zmíněná sada pravidel:

1. **Závislosti**: Skripty vyhovují Pythonu 3. Všechny potřebné balíčky pro spuštění skriptů i analytických notebooků lze nainstalovat najednou ze souboru `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```
2. **Klíče API (Subscan)**: XCM analytické skripty (např. `pipeline.py`, `fill_costs.py`) vyžadují před spuštěním nastavení proměnné prostředí se Subscan API klíčem:
   ```bash
   export SUBSCAN_API_KEY="ZDE_VÁŠ_SUB_SCAN_KEY"
   ```
3. **Spuštění modulu samostatně**: 
   - Pro IBC: `python IBC_BP/ibc_download.py --src_chain osmosis`
   - Pro Axelar: `python Axelar/axelar_downloader.py batch --out-dir data/`
   - Pro XCM: `python XCM_BP/pipeline.py --chain polkadot --from 2024-01-01 --to 2024-03-01`
