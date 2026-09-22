import socket
import json
import uuid

HOST = "127.0.0.1"
PORT = 5000


def send_json(writer, data):
    """
    Serializează obiectul Python în JSON și îl trimite brokerului.
    Fiecare mesaj se termină cu \n deoarece protocolul nostru
    folosește JSON Lines.
    """
    message = json.dumps(
        data,
        ensure_ascii=False
    )

    writer.write(message + "\n")
    writer.flush()


def main():
    print("=" * 50)
    print("          PUBLISHER - PYTHON")
    print("=" * 50)

    try:
        # Creăm socket TCP.
        client_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        # Ne conectăm la Broker.
        client_socket.connect(
            (HOST, PORT)
        )

        print(f"Conectat la Broker: {HOST}:{PORT}")
        print()

        # Folosim makefile pentru a putea lucra ușor
        # cu mesaje terminate prin \n.
        reader = client_socket.makefile(
            "r",
            encoding="utf-8"
        )

        writer = client_socket.makefile(
            "w",
            encoding="utf-8"
        )

        while True:
            print()
            print("------------------------------------------")
            print("1 - Publică mesaj")
            print("2 - Trimite JSON invalid (test DLQ)")
            print("0 - Ieșire")
            print("------------------------------------------")

            option = input(
                "Alege opțiunea: "
            ).strip()

            # =====================================================
            # PUBLISH
            # =====================================================

            if option == "1":

                topic = input(
                    "Topic: "
                ).strip()

                content = input(
                    "Mesaj: "
                ).strip()

                if not topic:
                    print(
                        "Eroare: topicul nu poate fi gol."
                    )
                    continue

                if not content:
                    print(
                        "Eroare: mesajul nu poate fi gol."
                    )
                    continue

                # ID unic pentru fiecare mesaj.
                message_id = str(
                    uuid.uuid4()
                )

                message = {
                    "action": "publish",
                    "messageId": message_id,
                    "topic": topic,
                    "content": content
                }

                print()
                print("Trimit către Broker:")
                print(
                    json.dumps(
                        message,
                        ensure_ascii=False,
                        indent=4
                    )
                )

                send_json(
                    writer,
                    message
                )

                # Așteptăm răspunsul Brokerului.
                response_line = reader.readline()

                if not response_line:
                    print(
                        "Brokerul a închis conexiunea."
                    )
                    break

                try:
                    response = json.loads(
                        response_line
                    )

                    print()
                    print("Răspuns Broker:")

                    print(
                        json.dumps(
                            response,
                            ensure_ascii=False,
                            indent=4
                        )
                    )

                    if (
                        response.get("action")
                        == "publish_accepted"
                    ):
                        subscribers = response.get(
                            "subscribers",
                            0
                        )

                        print()
                        print(
                            f"Mesajul {message_id} "
                            "a fost acceptat."
                        )

                        if subscribers == 0:
                            print(
                                "Momentan nu există "
                                "subscriberi pentru acest topic."
                            )

                            print(
                                "Mesajul rămâne persistent "
                                "în Broker."
                            )
                        else:
                            print(
                                f"Subscriberi găsiți: "
                                f"{subscribers}"
                            )

                except json.JSONDecodeError:
                    print(
                        "Răspuns invalid primit "
                        "de la Broker:"
                    )

                    print(response_line)

            # =====================================================
            # INVALID JSON TEST
            # =====================================================

            elif option == "2":

                invalid_message = (
                    '{"action":"publish",'
                    '"topic":"sport",'
                    '"content":'
                )

                print()
                print(
                    "Trimit intenționat JSON invalid:"
                )

                print(invalid_message)

                writer.write(
                    invalid_message + "\n"
                )

                writer.flush()

                response_line = reader.readline()

                if not response_line:
                    print(
                        "Brokerul a închis conexiunea."
                    )
                    break

                print()
                print(
                    "Răspuns Broker:"
                )

                print(
                    response_line.strip()
                )

            # =====================================================
            # EXIT
            # =====================================================

            elif option == "0":

                print(
                    "Publisher închis."
                )

                break

            else:
                print(
                    "Opțiune invalidă."
                )

        writer.close()
        reader.close()
        client_socket.close()

    except ConnectionRefusedError:

        print()
        print(
            "Nu se poate realiza conexiunea "
            "cu Brokerul."
        )

        print(
            "Verifică dacă Brokerul este pornit "
            f"pe {HOST}:{PORT}."
        )

    except Exception as error:

        print()
        print(
            f"Eroare Publisher: {error}"
        )


if __name__ == "__main__":
    main()