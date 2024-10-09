from flask import Flask, request, jsonify, render_template, Response
import requests
import json


import pandas as pd

app = Flask(__name__)

OLLAMA_URL = "http://ollama:11434/api/generate"

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/executar', methods=['POST'])
def execute():
    data = request.json
    prompt = data.get('command')
 
    if not prompt:
        return jsonify({'resposta': 'Forneça um input!'}), 400

    headers = {
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "llama3.2:1b",
        "prompt": prompt,
        "stream": False
    }

    try:
        response = requests.post(OLLAMA_URL, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            response_json = response.json()
            return jsonify({ 'resposta': response_json.get('response') })
        else:
            return jsonify({'resposta': 'houveu um erro ao processar a resposta'})   
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/classificar')
def classificar():
    try:
        df = pd.read_json('dados_com_etl.json')
        df = df.head(50)
        df.drop(columns=['song', 'link'], inplace=True)
        if 'text' not in df.columns:
            return jsonify({'error': 'Coluna "text" não encontrada no CSV'}), 400
   

        classificacoes = []
        for letra in df['text']:
            if pd.isna(letra):
                classificacoes.append('Neutra')
                continue
            
            headers = {
                "Content-Type": "application/json"
            }

            data = {
                "model": "llama3.2:1b",
                "prompt": f"Apenas classifique a letra da musica a seguir como resposta 'Positiva', 'Negativa' ou 'Neutra', me dê como resposta apenas uma única palavra dessas 3 anteriores seguindo o padrão que eu forneci: \n\n  {letra}",
                "stream": False
            }

            try:
                response = requests.post(OLLAMA_URL, headers=headers, data=json.dumps(data))
                if response.status_code == 200:
                    response_json = response.json()
                    print('resposta modelo: ', response_json.get('response'))
                    resposta_classificacao = response_json.get('response', 'Neutra').strip().lower()

                    # Convertendo a resposta em uma classificação válida
                    if 'positiva' in resposta_classificacao:
                        classificacoes.append('Positiva')
                    elif 'negativa' in resposta_classificacao:
                        classificacoes.append('Negativa')
                    else:
                        classificacoes.append('Neutra')
                else:
                    classificacoes.append('Neutra') 
            except Exception as e:
                classificacoes.append('Neutra')  

        df['classification'] = classificacoes
        classificacao_counts = df['classification'].value_counts().to_dict()

        output = 'dados_com_etl_classificados.json'
        df.to_json(output, orient='records', indent=4, force_ascii=False)

        return jsonify({
            'respotas': f'Classificação concluida com sucesso. Arquivo salvo em {output}',
            'contagem': classificacao_counts
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
