# Project status — inżynierka

## Stan na 2026-08-30

Projekt realizuje prognozowanie dziennej sprzedaży na poziomie `StoreNo + IDX` przy użyciu XGBoost.

## Aktualny zakres danych

- 2 sklepy,
- 200 wybranych IDX na sklep,
- 400 serii `StoreNo + IDX`,
- po przebudowie panelu: 288 346 rekordów,
- `Sales = 0`: 128 168 rekordów (44.45%),
- `Sales > 0`: 160 178 rekordów (55.55%),
- OOS = 1: 8 664 rekordy,
- promo = 1: 172 815 rekordów.

## Najważniejsze poprawki względem pierwszego prototypu

1. Usunięto `SalesValue` z wejścia modelu z powodu target leakage.
2. OOS nie jest używany jako cecha modelu.
3. Rekordy z `OOS = 1` są wyłączone z treningu i ewaluacji.
4. Historyczna sprzedaż z dni OOS jest ustawiana na `NaN`, aby nie tworzyć fałszywych zer w lagach.
5. Dodano lagi `1`, `7`, `14`, `28` dni.
6. Lagi są liczone po dokładnej dacie, a nie przez zwykłe przesunięcie liczby wierszy.
7. Dodano `history_mean_4lags`.
8. Zbudowano pełny dzienny panel aktywności produktu na podstawie planogramu oraz rzeczywistych rekordów sprzedaży.
9. Liczba braków `lag_7` spadła z 54 183 do 14 361 rekordów.
10. Promocje są przypisywane po `IDX + DateStart/DateEnd`, a nie po ID rekordu sprzedaży.
11. Dodano baseline `t-1` oraz seasonal naive `t-7`.
12. MAPE zastąpiono metrykami MAE, RMSE i WAPE.
13. Kategorie są kodowane One-Hot na podstawie wyłącznie zbioru treningowego.

## Główny model

XGBoost + OneHotEncoder + `promo`.

Cechy numeryczne:

- `dow`,
- `month`,
- `day_of_month`,
- `week_of_year`,
- `promo`,
- `lag_1`,
- `lag_7`,
- `lag_14`,
- `lag_28`,
- `history_mean_4lags`.

Cechy kategoryczne:

- `StoreNo`,
- `Brand`,
- `DIV2`,
- `IDX`.

Aktualnie po One-Hot: 385 cech.

## Wyniki główne

| Model | MAE | RMSE | WAPE |
|---|---:|---:|---:|
| Globalny XGBoost | 1.5429 | 3.1196 | 77.73% |
| Naive t-1 | 1.9919 | 4.3761 | 100.09% |
| Seasonal naive t-7 | 1.9896 | 4.4486 | 100.23% |

Cutoff train/test: `2024-08-05`.

Train: 214 698 rekordów po wyłączeniu OOS.
Test: 55 089 rekordów.
Ewaluacja po wyłączeniu OOS: 54 369 rekordów.

## Feature experiments

### history_mean_4lags

Pozostawiona — poprawiała wyniki względem samych lagów.

### promo

Pozostawiona.

Eksperyment One-Hot:

- bez promo: MAE 1.5509, RMSE 3.1546, WAPE 78.13%,
- z promo: MAE 1.5429, RMSE 3.1196, WAPE 77.73%.

### discount_percent

Wyłączona.

Z `promo + discount_percent`:

- MAE 1.5612,
- RMSE 3.1668,
- WAPE 78.65%.

Nie poprawiała modelu.

## Eksperyment kodowania kategorii

- ręczne kody liczbowe: MAE 1.5505, RMSE 3.1641, WAPE 78.11%,
- native categorical XGBoost: MAE 1.5722, RMSE 3.2124, WAPE 79.20%,
- OneHotEncoder: MAE 1.5509, RMSE 3.1546, WAPE 78.13%.

Wybrano OneHotEncoder ze względu na poprawną interpretację zmiennych nominalnych i brak sztucznego porządku liczbowego.

## Analiza rotacji

Segmentacja wyznaczana wyłącznie na trainie według średniej dziennej sprzedaży.

Progi:

- low / medium: 1.1678445,
- medium / high: 1.6020202.

Liczba serii:

- low: 134,
- medium: 133,
- high: 133.

### Globalny model wg grup

| Grupa | MAE | RMSE | WAPE |
|---|---:|---:|---:|
| low | 0.9766 | 1.3545 | 90.69% |
| medium | 1.2187 | 1.8331 | 84.85% |
| high | 2.4555 | 4.9309 | 70.61% |

WAPE spada wraz ze wzrostem rotacji. Wysoki globalny WAPE wynika częściowo z dużego udziału sprzedaży zerowej i niskich wartości sprzedaży w wolno rotujących seriach.

### Osobne modele segmentowe

| Grupa | Globalny XGB WAPE | Segmentowany XGB WAPE |
|---|---:|---:|
| low | 90.69% | 90.19% |
| medium | 84.85% | 86.25% |
| high | 70.61% | 71.44% |

Globalnie:

- globalny XGBoost: WAPE 77.73%,
- segmentowany XGBoost: WAPE 78.45%.

Wniosek: osobne modele nie przyniosły poprawy globalnej. Minimalna korzyść dla low rotation nie kompensuje pogorszenia dla medium i high.

## Decyzje finalne

- Głównym modelem pracy pozostaje jeden globalny XGBoost.
- OneHotEncoder pozostaje finalnym sposobem obsługi kategorii.
- `promo` pozostaje w modelu.
- `discount_percent` pozostaje poza modelem.
- Eksperyment segmentacji zostaje opisany jako wynik dodatkowy.
- `MLScript_segmented.py` pozostaje w repo jako eksperyment porównawczy, ale nie jest głównym pipeline'em.

## Kolejne zadania

1. Uporządkować finalne nazwy plików i usunąć puste/prototypowe skrypty.
2. Uzupełnić README i zależności.
3. Zachować finalne raporty i wykresy używane w pracy.
4. Przygotować część pisemną:
   - wstęp,
   - cel i zakres pracy,
   - podstawy teoretyczne,
   - opis danych,
   - pipeline ETL,
   - metodologia modelowania,
   - eksperymenty i wyniki,
   - podsumowanie.
5. Przed finalnym oddaniem rozważyć jeden końcowy eksperyment strojenia hiperparametrów, ale tylko jeśli czas pozwala.
