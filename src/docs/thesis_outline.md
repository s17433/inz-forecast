# Proponowany układ pracy inżynierskiej

## 1. Wstęp

- znaczenie prognozowania popytu i sprzedaży w handlu detalicznym,
- konsekwencje niedoszacowania i przeszacowania popytu,
- rola danych historycznych, promocji i dostępności produktu,
- krótkie przedstawienie celu pracy.

## 2. Cel i zakres pracy

### 2.1. Cel główny

Zaprojektowanie i ocena procesu prognozowania dziennej sprzedaży na poziomie sklep–produkt z wykorzystaniem modelu XGBoost.

### 2.2. Cele szczegółowe

- przygotowanie danych z wielu źródeł,
- utworzenie pełnego panelu dziennego sklep–produkt,
- uwzględnienie problemu OOS,
- budowa cech historycznych i kalendarzowych,
- porównanie modelu ML z metodami bazowymi,
- analiza wpływu promocji,
- analiza jakości prognoz w zależności od rotacji produktu.

### 2.3. Pytania badawcze

1. Czy XGBoost daje niższy błąd prognozy niż naive t-1 i seasonal naive t-7?
2. Czy informacja o aktywnej promocji poprawia jakość prognozy?
3. Czy jakość prognozy zależy od poziomu rotacji serii sklep–produkt?
4. Czy trenowanie osobnych modeli dla segmentów rotacji poprawia wynik względem jednego modelu globalnego?

## 3. Podstawy teoretyczne

### 3.1. Prognozowanie popytu i sprzedaży
### 3.2. Szeregi czasowe
### 3.3. Specyfika sprzedaży detalicznej
- sezonowość,
- promocje,
- zerowa sprzedaż,
- intermittent demand,
- braki dostępności OOS.

### 3.4. Metody bazowe
- naive forecast,
- seasonal naive.

### 3.5. Uczenie maszynowe w prognozowaniu
### 3.6. Gradient boosting i XGBoost
### 3.7. Metryki MAE, RMSE i WAPE

## 4. Charakterystyka danych

### 4.1. Źródła danych
- SalesChStores,
- PlanogramChStores,
- Promotions,
- OutOfStock,
- IDXs,
- ChosenIDXs,
- ChosenStores.

### 4.2. Zmienna docelowa
- Sales.

### 4.3. Problemy jakości danych
- braki,
- OOS,
- brak dni bez sprzedaży w pierwotnym zbiorze,
- target leakage przez SalesValue,
- promocje pierwotnie wiązane po ID sprzedaży.

## 5. Projekt i implementacja procesu przetwarzania danych

### 5.1. Konwersja danych do Parquet
### 5.2. Czyszczenie danych
### 5.3. Budowa panelu sklep–produkt–dzień
### 5.4. Łączenie sprzedaży, planogramu, OOS i promocji
### 5.5. Obsługa promocji po IDX i zakresie dat
### 5.6. Przygotowanie zbioru finalnego

## 6. Budowa modelu prognostycznego

### 6.1. Podział czasowy train/test
### 6.2. Obsługa OOS
### 6.3. Feature engineering
- dow,
- month,
- day_of_month,
- week_of_year,
- promo,
- lag_1,
- lag_7,
- lag_14,
- lag_28,
- history_mean_4lags.

### 6.4. OneHotEncoder
### 6.5. XGBoost
### 6.6. Metody bazowe

## 7. Eksperymenty i wyniki

### 7.1. Wynik modelu globalnego
### 7.2. Porównanie z baseline
### 7.3. Eksperyment z cechą promo
### 7.4. Eksperyment z discount_percent
### 7.5. Porównanie sposobów kodowania kategorii
### 7.6. Analiza low / medium / high rotation
### 7.7. Globalny vs segmentowany XGBoost
### 7.8. Interpretacja wyników i ograniczenia

## 8. Podsumowanie

- odpowiedzi na pytania badawcze,
- najważniejsze rezultaty,
- ograniczenia badania,
- możliwe kierunki rozwoju:
  - większa liczba sklepów i produktów,
  - tuning hiperparametrów,
  - Croston/SBA/TSB dla intermittent demand,
  - dodatkowe dane kalendarzowe i zewnętrzne,
  - bardziej rozbudowane cechy promocyjne.
