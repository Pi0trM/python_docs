import csv
import json
import random
from datetime import datetime, timedelta

def generate_data():
    """
    Generuje numery PESEL, imiona i nazwiska kandydatów, kod pocztowy, numer telefonu, średnią ocene z matur, status aplikacji, datę jej złożenia oraz wybrany wydział z jego numerem ID.
    Na podstawie 10 cyfry numeru PESEL wybierana jest płeć kandydata, co ma wpływ na wybierane imiona i formę nazwisk.
    Średnia ocena z matur może mieć wpływ na status aplikacji.
    """
    imiona_m = ["Adam", "Aleksander", "Andrzej", "Bartosz", "Borys", "Cezary", "Damian", "Dawid", "Eryk", "Fabian", 
                "Filip", "Grzegorz", "Hubert", "Igor", "Jakub", "Kamil", "Krzysztof", "Leon", "Maciej", "Mikołaj", 
                "Nikodem", "Oskar", "Patryk", "Rafał", "Szymon", "Tobiasz", "Tymon", "Wiktor", "Zbigniew", "Zygmunt"]
    
    imiona_f = ["Alicja", "Anna", "Barbara", "Beata", "Cecylia", "Dominika", "Eliza", "Gabriela", "Hanna", "Iga", 
                "Jagoda", "Julia", "Karolina", "Katarzyna", "Kinga", "Laura", "Lena", "Lidia", "Magdalena", 
                "Marcelina", "Milena", "Natalia", "Natasza", "Oliwia", "Paulina", "Roksana", "Sandra", "Sylwia", 
                "Weronika", "Zofia"]
    
    nazwiska_m = ["Kowalski", "Nowak", "Wiśniewski", "Dąbrowski", "Lewandowski", "Wójcik", "Kamiński", "Zieliński", "Szymański", "Woźniak",
                  "Kozłowski", "Jankowski", "Mazur", "Kwiatkowski", "Wróbel", "Piotrowski", "Grabowski", "Zając", "Król", "Pawlak",
                  "Michalski", "Adamczyk", "Nowicki", "Dudek", "Wieczorek", "Jabłoński", "Górski", "Walczak", "Rutkowski", "Michalak",
                  "Sikora", "Ostrowski", "Baran", "Pietrzak", "Wasilewski", "Czarnecki", "Szulc", "Makowski", "Kubiak", "Wilk",
                  "Grzelak", "Kucharski", "Wróblewski", "Lis", "Kaczmarek", "Mazurek", "Sobczak", "Czerwiński", "Andrzejewski", "Stępień",
                  "Malinowski", "Urban", "Tokarski", "Tomczak", "Janik", "Bednarek", "Skiba", "Borowski", "Musiał", "Krajewski",
                  "Polak", "Matusiak", "Gajewski", "Orłowski", "Kulesza", "Wilczyński", "Janowski", "Głowacki", "Sadowski", "Staniszewski"]
    
    nazwiska_f = [nazwisko[:-1] + "a" if nazwisko.endswith("i") else nazwisko for nazwisko in nazwiska_m]
    
    #Generujemy PESEL
    pesel = ''.join(str(random.randint(0, 9)) for _ in range(11))
    
    #Ustalamy płeć na podstawie dziesiątej cyfry w PESELu i dobieramy odpowiednie imię i formę nazwiska.
    if int(pesel[9]) % 2 == 0:
        imie = random.choice(imiona_f)
        nazwisko = random.choice(nazwiska_f)
    else:
        imie = random.choice(imiona_m)
        nazwisko = random.choice(nazwiska_m)
       
    #Generujemy kod pocztowy, numer telefonu, średnią maturalną, status aplikacji, datę rekrutacji oraz wydzial
    kodpocztowy = f"{random.randint(00, 99):02}-{random.randint(000, 999):03}"
    
    telefon = f"+48 {random.randint(500, 899)}-{random.randint(100, 999)}-{random.randint(100, 999)}"
     
    sredniamaturalna = round(random.uniform(50.00, 90.00), 2)
        
    statusaplikacji = ""
    if sredniamaturalna > 80:
        statusaplikacji = random.choice(["oczekuje", "zaakceptowany"])
    elif sredniamaturalna < 60:
        statusaplikacji = "odrzucony"
    else:
        statusaplikacji = random.choice(["oczekuje", "zaakceptowany", "odrzucony"])
     
    datarekrutacji = (datetime.today() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
    
    id_wydzialu = random.choice([0, 1, 2, 3, 4, 5, 6, 7])
    wydzialy = ["Informatyka", "Matematyka", "Biologia", "Fizyka", "Prawo", "Ekonomia", "Filologia", "Medycyna"]
    wydzial_nazwa = wydzialy[id_wydzialu]
    
    return [pesel, imie, nazwisko, kodpocztowy, telefon,
            datarekrutacji, sredniamaturalna, statusaplikacji,
            id_wydzialu, wydzial_nazwa]

def generate_csv():
    """
    Generuje plik CSV z danymi 300 kandydatów.
    """
    file_name = "kandydaci.csv"
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["pesel", "imie", "nazwisko", "kodpocztowy", "telefon",
                         "datarekrutacji", "sredniamaturalna", "statusaplikacji",
                         "idwydzialu", "nazwawydzialu"])
         
        for _ in range(300):
            writer.writerow(generate_data())
    
    print(f"Plik '{file_name}' został wygenerowany.")

def generate_json():
    """
    Generuje plik JSON z danymi 300 kandydatów.
    """
    file_name = "kandydaci.json"
    dane = []
    
    for _ in range(300):
        pesel, imie, nazwisko, kodpocztowy, telefon, datarekrutacji, sredniamaturalna, statusaplikacji, id_wydzialu, wydzial_nazwa = generate_data()
        kandydat = {
            "pesel": pesel,
            "imie": imie,
            "nazwisko": nazwisko,
            "kodpocztowy": kodpocztowy,
            "telefon": telefon,
            "datarekrutacji": datarekrutacji,
            "sredniamaturalna": sredniamaturalna,
            "statusaplikacji": statusaplikacji,
            "idwydzialu": id_wydzialu,
            "nazwawydzialu": wydzial_nazwa
        }
        
        dane.append(kandydat)
    
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(dane, f, ensure_ascii=False, indent=4)
    
    print(f"Plik '{file_name}' został wygenerowany.")

