from config import RAW

def change_field_separator(input_file, output_file, original_sep=',', new_sep=';'):
    with open(input_file, 'r', encoding='utf-8') as fin, open(output_file, 'w', encoding='utf-8') as fout:
        for line in fin:
            # Zamień wszystkie przecinki na średniki, poza separatorami dziesiętnymi
            if 'stock_price_net' in line:
                # Zakładamy, że nagłówek zawiera 'stock_price_net'
                fout.write(line.replace(original_sep, new_sep))
            else:
                # Zamień tylko separator pól, zostawiając przecinki w liczbach
                parts = line.strip().split(original_sep)
                if len(parts) == 5:
                    # Ostatni element to część dziesiętna, więc połącz go z poprzednim
                    fixed_stock_price = parts[-2] + ',' + parts[-1]
                    fixed_line = new_sep.join(parts[:-2] + [fixed_stock_price])
                    fout.write(fixed_line + '\n')
                else:
                    fout.write(new_sep.join(parts) + '\n')

# Użycie:
input_file = RAW/"PlanogramChStores.txt"
output_file = RAW/"PlanogramChStores_fixed_sep.txt"
change_field_separator(input_file, output_file)