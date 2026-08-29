# Prognozowanie dziennej sprzedaży w handlu detalicznym

Projekt inżynierski dotyczący prognozowania dziennej sprzedaży na poziomie **sklep–produkt** z wykorzystaniem danych historycznych, informacji kalendarzowych, promocji oraz modelu XGBoost.

## Cel projektu

Celem projektu jest przygotowanie kompletnego procesu przetwarzania danych i budowy modelu prognostycznego dla sprzedaży detalicznej. Model jest oceniany na podziale czasowym i porównywany z prostymi metodami bazowymi:

- naive `t-1` — prognoza równa sprzedaży z dnia poprzedniego,
- seasonal naive `t-7` — prognoza równa sprzedaży sprzed siedmiu dni.

Główny model wykorzystuje XGBoost oraz kodowanie One-Hot dla cech kategorycznych.

## Główne założenia metodologiczne

- zmienną docelową jest `Sales`,
- jedna seria czasowa odpowiada parze `StoreNo + IDX`,
- podział train/test jest wykonywany chronologicznie,
- `SalesValue` nie jest używane jako cecha z uwagi na ryzyko target leakage,
- `OOS` nie jest cechą wejściową modelu,
- dni z `OOS = 1` są wyłączane z treningu i ewaluacji,
- sprzedaż z dni OOS nie jest wykorzystywana do budowania lagów,
- lagi są liczone po dokładnych datach kalendarzowych: `1`, `7`, `14`, `28` dni,
- `promo` jest używane jako cecha,
- `discount_percent` został wyłączony po eksperymencie ablation, ponieważ nie poprawiał jakości prognozy,
- `StoreNo`, `IDX`, `Brand` i `DIV2` są kodowane przez `OneHotEncoder` dopasowany wyłącznie na zbiorze treningowym.

## Wynik głównego modelu

Dla aktualnego eksperymentu na 2 sklepach i 400 seriach sklep–produkt:

| Model | MAE | RMSE | WAPE |
|---|---:|---:|---:|
| XGBoost | **1.5429** | **3.1196** | **77.73%** |
| Naive t-1 | 1.9919 | 4.3761 | 100.09% |
| Seasonal naive t-7 | 1.9896 | 4.4486 | 100.23% |

Model XGBoost osiąga niższy błąd od obu metod bazowych.

## Analiza rotacji

Serie zostały dodatkowo podzielone na grupy `low`, `medium` i `high` na podstawie średniej dziennej sprzedaży wyznaczonej wyłącznie na danych treningowych.

Progi w aktualnym eksperymencie:

- low / medium: `1.1678` szt./dzień,
- medium / high: `1.6020` szt./dzień.

WAPE globalnego XGBoost:

- low: `90.69%`,
- medium: `84.85%`,
- high: `70.61%`.

Osobne modele dla trzech segmentów nie poprawiły wyniku globalnego. Segmentowany XGBoost uzyskał WAPE `78.45%`, wobec `77.73%` dla jednego modelu globalnego.

## Struktura projektu

```text
inz-forecast/
├── data/
│   ├── raw/                # dane źródłowe, poza Git
│   ├── processed/          # pliki po konwersji, poza Git
│   └── final/              # dane po ETL, poza Git
├── docs/
│   └── project_status.md   # stan projektu i decyzje metodologiczne
├── reports/
│   ├── plots/
│   └── *.csv / *.json
├── src/
│   ├── config.py
│   ├── fix_delimiter.py
│   ├── convert_to_parquet.py
│   ├── cleaning.py
│   ├── etl_merge.py
│   ├── AnalyseData.py
│   ├── MLScript.py
│   └── MLScript_segmented.py
├── requirements.txt
└── README.md
```

## Kolejność uruchamiania

Po przygotowaniu plików źródłowych w `data/raw`:

```powershell
python src\fix_delimiter.py
python src\convert_to_parquet.py
python src\cleaning.py
python src\etl_merge.py
python src\AnalyseData.py
python src\MLScript.py
```

Eksperyment z osobnymi modelami dla grup rotacji:

```powershell
python src\MLScript_segmented.py
```

## Środowisko

Rekomendowany Python: **3.12**.

Utworzenie środowiska na Windows:

```powershell
python -m venv forecastingenv
forecastingenv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Dane

Dane użyte w pracy nie są przechowywane w repozytorium. Projekt wykorzystuje m.in. dane o:

- sprzedaży dziennej,
- produktach i hierarchii produktowej,
- sklepach,
- brakach dostępności OOS,
- planogramach,
- promocjach.

## Metryki

W projekcie używane są:

- **MAE** — średni błąd bezwzględny,
- **RMSE** — silniej karze duże błędy,
- **WAPE** — główna względna miara błędu, odporna na pojedyncze obserwacje `Sales = 0` w przeciwieństwie do klasycznego MAPE.

## Status

Główny pipeline i bazowy eksperyment są gotowe. Kolejne prace dotyczą głównie dokumentacji projektu, analizy wyników oraz przygotowania części pisemnej pracy inżynierskiej.
