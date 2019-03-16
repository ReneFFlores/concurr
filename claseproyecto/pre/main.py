import json
import re


def main():
    # informacion necesaria del usuario
    nombre_dataset = input('archivo de dataset: ') or 'dataset.json'
    campo = input('campo del dataset: ') or 'reviewText'
    nombre_resultado = input('archivo de resultado: ') or 'dataset.txt'
    nombre_ignorar = input('archivo de palabras ignoradas: ') or 'ignorar.txt'

    # abrir archivos
    dataset = open(nombre_dataset, 'r')
    resultado = open(nombre_resultado, 'w')
    archivo_ignorar = open(nombre_ignorar, 'r')

    # cargar palabras ignoradas en un arreglo
    ignorar = []
    for palabra in archivo_ignorar.read().split('\n'):
        if palabra != '':
            ignorar.append(f' {palabra} ')

    # preprocesado
    for linea in dataset:
        # quitar caracteres especiales
        json_linea = json.loads(linea)
        texto = ' ' + re.sub('[^a-zA-Z0-9 \']', ' ', json_linea[campo]).lower()

        # quitar palabras ignoradas
        for ignorada in ignorar:
            while ignorada in texto:
                texto = texto.replace(ignorada[:-1], '')

        resultado.write(texto + '\n')

    print(f'resultado guardado en {nombre_resultado}')

if __name__ == '__main__':
    main()
