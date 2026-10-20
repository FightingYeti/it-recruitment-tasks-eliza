/**
 * TWOJE ZADANIE: zaimplementuj testy.
 *
 * Aplikacja (producer + consumer) jest już gotowa i działająca — Twoim
 * zadaniem jest napisanie testów E2E weryfikujących jej zachowanie.
 *
 * Masz do dyspozycji gotowe helpery z `src/kafkaHelpers.ts` (opisane
 * w komentarzach tego pliku) — możesz z nich korzystać, ale nie musisz:
 * jeśli uważasz, że lepiej napisać coś inaczej, zrób to i uzasadnij w README.
 *
 */
import { Producer } from "kafkajs";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  ORDERS_TOPIC,
  Order,
  collectMessages,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  sendRaw,
  waitForMessageWithKey,
} from "../src/kafkaHelpers";
import { randomUUID } from "crypto";

let producer: Producer;

beforeAll(async () => {
  producer = await getProducer();
});

afterAll(async () => {
  await disconnectProducer();
});

describe("przepływ przetwarzania zamówień przez Kafkę", () => {
  const processed = (order: Order): Order => ({ ...order, status: "PROCESSED" });

  test("zamówienie powinno zostać przeprocesowane", async () => {
    // given
    const order: Order = makeTestOrder();

    // when
    await sendOrder(producer, order);

    // then
    const newOrder = await waitForMessageWithKey(ORDERS_TOPIC, order.orderId);
    expect(newOrder).toEqual(order);

    // and
    const processedOrder = await waitForMessageWithKey(ORDERS_PROCESSED_TOPIC, order.orderId);
    expect(processedOrder).toEqual(processed(order));
  });

  test("zamówienie ze statusem innym niż 'NEW' powinno zostać przeprocesowane (chociaż ja bym to wysyłała na DLQ :))", async () => {
    // given
    const order: Order = makeTestOrder({ status: "UNKNOWN" });

    // when
    await sendOrder(producer, order);

    // then
    const processedOrder = await waitForMessageWithKey(ORDERS_PROCESSED_TOPIC, order.orderId);
    expect(processedOrder).toEqual(processed(order));
  });

  test("dwa takie same zamówienia powinny zostać przeprocesowane", async () => {
    // given
    const order: Order = makeTestOrder();

    // when
    await sendOrder(producer, order);
    await sendOrder(producer, order);

    // then
    const processedOrder = await collectMessages(ORDERS_PROCESSED_TOPIC, (msg) => msg.orderId === order.orderId, 2);
    expect(processedOrder[0]).toEqual(processed(order));
    expect(processedOrder[1]).toEqual(processed(order));
  });
});

describe("walidacje - wysłanie wiadomości na DLQ", () => {
  let key: string;

  beforeEach(async () => {
    key = randomUUID();
  });
 
  const expectDlqError = async (key: string, expected: string) => {
    const dlqMessage = await waitForMessageWithKey(ORDERS_DLQ_TOPIC, key);
    expect(dlqMessage.error).toEqual(expected);
  };

  test("zamówienie z niepoprawnym JSON nie powinno zostać przeprocesowane", async () => {
    // given
    const message = "string";

    // when
    await sendRaw(producer, message, key);

    // then
    const dlqOrder = await waitForMessageWithKey(ORDERS_DLQ_TOPIC, key);
    expect(dlqOrder.error).toContain("Niepoprawny JSON");
  });

  test("zamówienie z brakującymi wszystkimi polami nie powinno zostać przeprocesowane", async () => {
    // given
    const message = `{"unknown": "unknown"}`;

    // when
    await sendRaw(producer, message, key);

    // then
    await expectDlqError(key, "Brakujące pola: [orderId, customer, amount, status]");
  });

  test.each(["orderId", "customer", "amount", "status"] as const)("zamówienie z brakującym polem %s nie powinno zostać przeprocesowane", async (missingField) => {
    // given
    const { [missingField]: _omitted, ...message } = makeTestOrder();

    // when
    await sendRaw(producer, JSON.stringify(message), key);

    // then
    await expectDlqError(key, `Brakujące pola: [${missingField}]`);
  });

  test("zamówienie z niepoprawnym typem amount nie powinno zostać przeprocesowane", async () => {
    // given
    const message = { ...makeTestOrder(), amount: "test" };

    // when
    await sendRaw(producer, JSON.stringify(message), key);

    // then
    await expectDlqError(key, "Pole 'amount' musi być liczbą.");
  });

  test("zamówienie z niepoprawnym typem orderId nie powinno zostać przeprocesowane", async () => {
    // given
    const message = { ...makeTestOrder(), orderId: 1 };

    // when
    await sendRaw(producer, JSON.stringify(message), key);

    // then
    await expectDlqError(key, "Pole 'orderId' musi być stringiem.");
  });
});
