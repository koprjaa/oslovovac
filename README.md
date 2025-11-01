# Oslovení

Nástroj pro hromadné zpracování jmen a získávání jejich oslovení v 5. pádě (vokativ) z webu sklonuj.cz. Zpracovává velké CSV soubory s jmény asynchronně s možností obnovení po přerušení.

## Popis

Aplikace načte CSV soubor s jmény a pro každé jméno získá jeho oslovení pomocí API sklonuj.cz. Výsledky ukládá do nového CSV souboru spolu s odděleným jménem a příjmením. Podporuje checkpoint systém pro obnovení zpracování po přerušení a adaptivní mechanismy pro optimalizaci rychlosti zpracování.

## Instalace

1. Naklonujte repozitář:
```bash
git clone https://github.com/yourusername/osloveni.git
cd osloveni
```

2. Vytvořte virtuální prostředí a aktivujte ho:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# nebo
venv\Scripts\activate     # Windows
```

3. Nainstalujte závislosti:
```bash
pip install -r requirements.txt
```

## Použití

1. Připravte vstupní CSV soubor `jmena.csv` s následující strukturou:
```csv
ID,Trade_Name
1,Jan Novák
2,Petr Svoboda
3,Mgr. Jana Dvořáková
```

2. Spusťte aplikaci:
```bash
python main.py
```

3. Výsledky budou uloženy do souboru `jmena_s_oslovenim.csv`:
```csv
ID,Trade_Name,Oslovení,Oslovení jméno,Oslovení příjmení,Chyba
1,Jan Novák,Jane Nováku,Jane,Nováku,
2,Petr Svoboda,Petře Svobodo,Petře,Svobodo,
3,Mgr. Jana Dvořáková,Mgr. Jano Dvořáková,Mgr. Jano,Dvořáková,
```

Aplikace automaticky obnoví zpracování z posledního checkpointu při přerušení (Ctrl+C).

## Licence

MIT License - viz [LICENSE](LICENSE) soubor.

