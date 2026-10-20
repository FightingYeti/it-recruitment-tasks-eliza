## Wymagania

- Docker + Docker Compose v2 (`docker compose`, nie `docker-compose`).
- Wolny port `9092` lokalnie (Kafka).
- Do pracy lokalnej nad testami (poza Dockerem): Node.js 20+.

## Uruchomienie środowiska

```bash
# Zbuduj obrazy i uruchom Kafkę + inicjalizację topiców + konsumenta
docker compose up --build -d

# Sprawdź logi konsumenta (powinien czekać na wiadomości)
docker compose logs -f consumer
```

## Ręczna demonstracja działania aplikacji (opcjonalnie, nie jest wymagane do zadania)

```bash
docker compose --profile demo run --rm producer
docker compose logs -f consumer
```

## Uruchomienie testów (tu pracujesz)

```bash
docker compose --profile test run --rm tests
```

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera:

```bash
cd tests
npm install
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 npm test
```

Sprawdzenie typów bez uruchamiania testów:

```bash
cd tests
npx tsc --noEmit
```

## Zatrzymanie i sprzątanie środowiska

```bash
docker compose down -v
```

(`-v` czyści też dane Kafki — przydatne, gdy chcesz zacząć testy od zera).


---

## Twoje notatki (uzupełnij przed oddaniem zadania)

### Uwagi do uruchomienia
_(jeśli coś zmieniłeś/aś względem domyślnej konfiguracji, opisz to tutaj)_
1. docker uruchamia mi kontenery na linuxie, a skrypt init-topics.sh ma windowsowe znaki końca linii i trzeba było zmienić format na unix - być może przy uruchomieniu z tego repo na kontenerach windowsowych trzeba będzie to przywrócić

### Podejście do testowania
_(2–4 zdania: jak podszedłeś/aś do problemu, jakie decyzje podjąłeś/aś)_
1. na początek sprawdziłam co się świeci w kodzie i dociągnęłam brakujące zależności, pobrałam wtyczkę Jest, żeby łatwiej było odpalać testy lokalnie
1. uruchomiłam aplikację, odpaliłam konsumenta, wysłałam wiadomości producentem, uruchomiłam UI Kafki, zobaczyłam co widać w logach i ogólnie zorientowałam się co i jak działa
1. powymyślałam przypadki testowe i patrząc na kod konsumenta sprawdziłam jakie są obecne walidacje (nie pisałam dodatkowych testów na walidacje, które nie istnieją, a mogłyby)
1. zamiast waitForMessage użyłam waitForMessageWithKey - w tej aplikacji to może bez znaczenia, ale wyszukiwanie po kluczu wydaje mi się wydajniejsze
1. nie sprawdzałam w testach dlq tego czy wiadomość nie trafia przypadkiem na topic processed - nie ma pewności czy wiadomość nie pojawi się po timeoutcie, a nie ma co robić długich timeoutów, które i tak nie dają pewnego wyniku

### Znane ograniczenia / co zrobiłbym/zrobiłabym inaczej mając więcej czasu
_(krótka lista)_
1. pozbycie się deprecated "moduleResolution": "node" z tsconfig.json
1. nie działało mi ustawienie zmiennej KAFKA_BOOTSTRAP_SERVERS=localhost:9092 przy puszczaniu testów lokalnie, więc do docker-compose dodałam dodatkowy port na słuchanie lokalne, ale jakbym miała więcej czasu to bym wolała ustalić czemu mi to nie działa i jak to rozwiązać jakoś legitnie
1. problem z consumer.disconnect() w metodach helpera - przy czytaniu wiadomości wpadało w jakąś pętlę i leciał timeout. Rozwiązanie tymczasowe z odasyncowaniem 'finish' zapodał Claude, ale wolałabym ogarnąć o co chodziło
1. dodałam oddzielną metodę waitForMessageWithKey prawie taką samą jak waitForMessage, a możnaby wydzielić część wspólną, żeby nie duplikować (albo w ogóle usunąć waitForMessage, bo jest nieużywana)
1. błędy walidacji możnaby brać z jakiegoś słownika zamiast wpisywać ręcznie stringi
