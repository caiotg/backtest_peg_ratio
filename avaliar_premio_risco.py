import pandas as pd
import numpy as np
import mplcyberpunk 
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import os
import matplotlib
import datetime

plt.style.use('cyberpunk')

class ResultadosPremios():

    def __init__(self, dicionarioFatores, dataFinalAnalise, caminhoImagens, caminhoPremiosRisco):
        
        self.dicinarioFatores = dicionarioFatores
        self.listaNomeFatores = []
        self.liquidez = []

        for key, item in dicionarioFatores.items():

            self.listaNomeFatores.append(key)
            self.liquidez.append(item)

        self.dataFinalAnalise = (datetime.datetime.strptime(dataFinalAnalise, '%Y-%m-%d')).date()
        self.caminhoPremiosRisco = caminhoPremiosRisco
        self.caminhoImagens = caminhoImagens

        os.chdir(caminhoImagens)

    def puxando_dados(self):

        listaDfs = []
        dataInicial = []

        for i, nomePremio in enumerate(self.listaNomeFatores):
            
            df = pd.read_parquet(f'{self.caminhoPremiosRisco}/{nomePremio}_{self.liquidez[i]}.parquet')
            df['data'] = pd.to_datetime(df['data']).dt.date

            listaDfs.append(df)
            dataInicial.append(min(df['data']))

        
        self.premiosDeRisco = pd.concat(listaDfs)
        dataInicial = max(dataInicial)

        self.premiosDeRisco = self.premiosDeRisco[(self.premiosDeRisco['data'] >= dataInicial) & (self.premiosDeRisco['data'] <= self.dataFinalAnalise)]

        self.premiosDeRisco = self.premiosDeRisco.assign(premio_fator = (1 + self.premiosDeRisco['primeiro_quartil'])/(1 + self.premiosDeRisco['quarto_quartil']))

        self.premiosDeRisco['primeiro_quartil'] = 1 + self.premiosDeRisco['primeiro_quartil']
        self.premiosDeRisco['segundo_quartil'] = 1 + self.premiosDeRisco['segundo_quartil']
        self.premiosDeRisco['terceiro_quartil'] = 1 + self.premiosDeRisco['terceiro_quartil']
        self.premiosDeRisco['quarto_quartil'] = 1 + self.premiosDeRisco['quarto_quartil']
        self.premiosDeRisco['universo'] = 1 + self.premiosDeRisco['universo']


    def retorno_quartis(self):

        for i, nomePremio in enumerate(self.listaNomeFatores):

            fator = self.premiosDeRisco[(self.premiosDeRisco['nome_premio'] == nomePremio) & (self.premiosDeRisco['liquidez'] == self.liquidez[i])]

            acumPrimeiroQuartil = (fator['primeiro_quartil'].cumprod() - 1).iloc[-1]
            acumSegundoQuartil = (fator['segundo_quartil'].cumprod() - 1).iloc[-1]
            acumTerceiroQuartil = (fator['terceiro_quartil'].cumprod() - 1).iloc[-1]
            acumQuartoQuartil = (fator['quarto_quartil'].cumprod() - 1).iloc[-1]

            fig, ax = plt.subplots(figsize = (4.75, 4))

            ax.bar(0, acumPrimeiroQuartil)
            ax.bar(1, acumSegundoQuartil)
            ax.bar(2, acumTerceiroQuartil)
            ax.bar(3, acumQuartoQuartil)

            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1))

            plt.xticks([0,1,2,3], ['1º Quartil', '2º Quartil', '3º Quartil', '4º Quartil'])
            plt.title(nomePremio)
            plt.savefig(f'{self.caminhoImagens}/barras_quartis_{nomePremio}_{self.liquidez[i]}')
            plt.close()

            fig, ax = plt.subplots(figsize= (4.75,4))

            ax.plot(fator['data'].values, (fator['primeiro_quartil'].cumprod() -1), label= '1º Quartil')
            ax.plot(fator['data'].values, (fator['segundo_quartil'].cumprod() - 1), label= '2ª Quartil')
            ax.plot(fator['data'].values, (fator['terceiro_quartil'].cumprod() - 1), label= '3º Quartil')
            ax.plot(fator['data'].values, (fator['quarto_quartil'].cumprod() - 1), label= '4º Quartil')
            ax.plot(fator['data'].values, (fator['universo'].cumprod() - 1), label= 'Universo')

            plt.legend()

            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1))

            plt.title(nomePremio)
            plt.savefig(f'{self.caminhoImagens}/linha_quartis_{nomePremio}_{self.liquidez[i]}')
            plt.close()

            fig, ax = plt.subplots(figsize= (4.75, 4))

            ax.plot(fator['data'].values, (fator['premio_fator'].cumprod() - 1))

            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1))
            
            plt.title(nomePremio + ' 1º Quartil minus 4º Quartil')
            plt.savefig(f'{self.caminhoImagens}/premio_de_risco_{nomePremio}_{self.liquidez[i]}')
            plt.close()

            serieMovel = pd.Series(data = fator['premio_fator'].rolling(12).apply(np.prod, raw= True) - 1)
            serieMovel.index = fator['data'].values
            serieMovel = serieMovel.dropna()

            fig, ax = plt.subplots(figsize= (4.75, 4))

            ax.plot(serieMovel.index, serieMovel.values)

            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1))

            plt.title(nomePremio + ' Janela móvel 12M')
            plt.savefig(f'{self.caminhoImagens}/movel_12m_premio_de_risco_{nomePremio}_{self.liquidez[i]}')


if __name__ == '__main__':

    dicionarioFatores ={
        'VALOR_PEG_RATIO': 1000000,
    }

    premios = ResultadosPremios(dataFinalAnalise= '2020-12-31', dicionarioFatores= dicionarioFatores, caminhoImagens= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\imagens', caminhoPremiosRisco= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\premios_risco')

    premios.puxando_dados()
    premios.retorno_quartis()















