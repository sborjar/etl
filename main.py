import sys
from datetime import date, datetime, timedelta
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.delivery import delivery

def main():
    
    print("█████████████████████████████████████████████████████████")
    print("█                                                       █")
    print("█                                                       █")
    print("█   CALLEVO INC.                                        █")
    print("█   ETL - EXTRACT, TRANSFORM AND LOAD                   █")
    print("█   CREATED: 2026-04-26                                 █")
    print("█                                                       █")
    print("█   Callevo Development Team                            █")
    print("█                                                       █")
    print("█                                                       █")
    print("█████████████████████████████████████████████████████████")
    
    if len(sys.argv) > 1:
        print(f"Argumentos recibidos: {sys.argv[1:]}")

        action = sys.argv[1]
        d1 = ""
        d2 = ""

        if action == 'r':
            """ Rango de fechas """
            d1 = sys.argv[2]
            d2 = sys.argv[3]

        elif action == 'd':
            """ Consulta por fecha unica """
            d1 = sys.argv[2]
        
        elif action == 'h':
            print("")
            print("")
            print("The Callevo ETL application accepts up to three arguments separated by spaces.")
            print("The first argument is the action:")
            print("")
            print("-r       Date range; must include two arguments: the start date and the end date.")
            print("-d       Process will run for a single day; a second argument is expected.")
            print("-h       Displays help")
            print("")
            print("La aplicacion procesara dia por dia generando estadisticas ")
            print("")
            print("Example:")
            print("python3 main.py r 2026-04-01 2026-04-26")
            print("python3 main.py d 2026-04-14")
            print("python3 main.py h")
            print("")
            print("")
            
        else:
            print("Wrong parameters.")
            exit(1)
        
        delivery(action,d1,d2)
                
    else:
        log("You need to set the execution parameters. Check the help by typing << python main.py h >>")
    

if __name__ == "__main__":
    main()
