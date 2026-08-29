# Rejestr eksperymentów

## Model bazowy po poprawie panelu

XGBoost, ręczne kodowanie kategorii, bez poprawnej obsługi promocji:
- MAE 1.3947
- RMSE 3.0458
- WAPE 70.26%

Wynik nie jest traktowany jako finalny benchmark z uwagi na arbitralne kodowanie kategorii i wcześniejszą logikę promocji.

## Kodowanie kategorii

| Wariant | MAE | RMSE | WAPE |
|---|---:|---:|---:|
| ręczne kody liczbowe | 1.5505 | 3.1641 | 78.11% |
| native categorical XGBoost | 1.5722 | 3.2124 | 79.20% |
| OneHotEncoder | 1.5509 | 3.1546 | 78.13% |

Decyzja: OneHotEncoder.

## Ablation promocji

| Wariant | MAE | RMSE | WAPE |
|---|---:|---:|---:|
| bez promo i discount_percent | 1.5509 | 3.1546 | 78.13% |
| + promo | 1.5429 | 3.1196 | 77.73% |
| + promo + discount_percent | 1.5612 | 3.1668 | 78.65% |

Decyzja: używać `promo`, nie używać `discount_percent`.

## Główny model vs baseline

| Model | MAE | RMSE | WAPE |
|---|---:|---:|---:|
| Globalny XGBoost | 1.5429 | 3.1196 | 77.73% |
| Naive t-1 | 1.9919 | 4.3761 | 100.09% |
| Seasonal naive t-7 | 1.9896 | 4.4486 | 100.23% |

## Rotacja

Progi na train:
- low/medium: 1.1678445 szt./dzień,
- medium/high: 1.6020202 szt./dzień.

| Grupa | XGB MAE | XGB RMSE | XGB WAPE |
|---|---:|---:|---:|
| low | 0.9766 | 1.3545 | 90.69% |
| medium | 1.2187 | 1.8331 | 84.85% |
| high | 2.4555 | 4.9309 | 70.61% |

## Globalny vs segmentowany

| Grupa | Globalny WAPE | Segmentowany WAPE |
|---|---:|---:|
| low | 90.69% | 90.19% |
| medium | 84.85% | 86.25% |
| high | 70.61% | 71.44% |

Globalnie:
- globalny XGBoost: MAE 1.5429, RMSE 3.1196, WAPE 77.73%,
- segmentowany XGBoost: MAE 1.5572, RMSE 3.1631, WAPE 78.45%.

Decyzja: głównym rozwiązaniem pozostaje jeden globalny model.
