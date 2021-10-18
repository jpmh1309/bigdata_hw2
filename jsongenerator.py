import json
import random 

cantidad_cajas = 5
cantidad_compras = 10

productos = [
    {"nombre":"manzana", "precio_unitario": 500},
    {"nombre":"pera", "precio_unitario": 400},
    {"nombre":"guayaba", "precio_unitario": 300},
    {"nombre":"naranja", "precio_unitario": 250},
    {"nombre":"sandia", "precio_unitario": 800},
    {"nombre":"pina", "precio_unitario": 700},
    {"nombre":"melon", "precio_unitario": 600},
    {"nombre":"mango", "precio_unitario": 300},
    {"nombre":"banano", "precio_unitario": 50},
    {"nombre":"limon", "precio_unitario": 50},
    {"nombre":"aguacate", "precio_unitario": 500},
    {"nombre":"melocoton", "precio_unitario": 400},
    {"nombre":"ciruela", "precio_unitario": 300},
    {"nombre":"higo", "precio_unitario": 200},
    {"nombre":"kiwi", "precio_unitario": 400},
    {"nombre":"papaya", "precio_unitario": 500}
]

def main():
    for i in range(cantidad_cajas):
        compras = []
        for j in range(cantidad_compras):
            cantidad_productos = random.randrange(1, len(productos))
            productos_seleccionados = random.sample(productos, cantidad_productos)
            for producto in productos_seleccionados: 
                producto["cantidad"] = random.randrange(1,5)
            compras.append(productos_seleccionados)
        
        print(compras)

        caja = {
            "numero_caja": i,
            "compras": compras
        }
        file_name = "datos/caja_{}.json".format(i)

        with open(file_name, 'w+') as outfile:
            json.dump(caja, outfile) 
            
if __name__ == "__main__":
    main()