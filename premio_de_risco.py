import pandas as pd
import numpy as np
from dateutil.relativedelta  import relativedelta
import os
import datetime

class PremioRisco():

    def __init__(self, nomePremio, liquidez=0, caminhoDados = None, caminhoSalvarArquivo = '.'):
        
        self.liquidez = liquidez
        self.caminhoSalvarArquivo = caminhoSalvarArquivo
        self.nomePremio = nomePremio

        if caminhoDados != None:

            os.chdir(caminhoDados)


    def pegando_dados_cotacoes(self):

        self.cotacoes = pd.read_parquet('cotacoes.parquet')
        self.cotacoes['id_dado'] = self.cotacoes['ticker'].astype(str) + '_' + self.cotacoes['data'].astype(str)
        self.cotacoes['data'] = pd.to_datetime(self.cotacoes['data']).dt.date

    def pegando_datas_possiveis(self):

        cotPetr = self.cotacoes[self.cotacoes['ticker'] == 'PETR4']

        cotPetr = cotPetr.sort_values('data', ascending= True)
        cotPetr = cotPetr.assign(year = pd.DatetimeIndex(cotPetr['data']).year)
        cotPetr = cotPetr.assign(month = pd.DatetimeIndex(cotPetr['data']).month)
        datasFinalMes = cotPetr.groupby(['year','month'])['data'].last()
        datasFinalMes = datasFinalMes.reset_index()

        self.datasFinalMes = datasFinalMes

    def filtrando_volume(self):

        dadosVolume = pd.read_parquet('volume_mediano.parquet')
        dadosVolume['id_dado'] = dadosVolume['ticker'].astype(str) + '_' + dadosVolume['data'].astype(str)
        dadosVolume = dadosVolume[['id_dado','valor']]
        dadosVolume.columns = ['id_dado','volumeMediano']
        self.cotacoes = pd.merge(self.cotacoes, dadosVolume, how= 'inner', on= 'id_dado')
        self.cotacoes = self.cotacoes[self.cotacoes['volumeMediano'] > self.liquidez]

    def descobrindo_mes_inicial(self):
        
        self.dfIndicadores = []

        pegRatio = pd.read_parquet('peg_ratio.parquet')
        pegRatio['data'] = pd.to_datetime(pegRatio['data']).dt.date
        self.dfIndicadores.append(pegRatio)

        datas = []
        for df in self.dfIndicadores:

            indicadorSemNa = df.dropna()
            petrIndicador = indicadorSemNa.query('ticker == "PETR4"')
            dataMinima = min(petrIndicador['data'])
            datas.append(dataMinima)
        
        dataMinimaGeral = max(datas)
        self.dataMinimaGeral = dataMinimaGeral + relativedelta(months=+2)
        self.listaDatasFinalMes = (self.datasFinalMes.query('data >= @self.dataMinimaGeral'))['data'].to_list()
        self.cotacoesFiltrado = self.cotacoes.query('data >= @self.dataMinimaGeral')
    
    def calculando_premio(self):

        colunas = ['primeiro_quartil','segundo_quartil','terceiro_quartil','quarto_quartil','universo']
        dfPremios = pd.DataFrame(columns=colunas, index= self.listaDatasFinalMes)

        listaDfs = [None] * 4

        for i, data in enumerate(self.listaDatasFinalMes):

            dfInfoPontuais = self.cotacoesFiltrado[self.cotacoesFiltrado['data'] == data][['ticker','preco_fechamento_ajustado','volume_negociado']]

            if i != 0:

                dfVendas = dfInfoPontuais[['ticker','preco_fechamento_ajustado']]
                dfVendas.columns = ['ticker','preco_fechamento_ajustado_posterior']
                listaRetornos = []

                for zeta, df in enumerate(listaDfs):

                    dfQuartil = pd.merge(df, dfVendas, how='inner', on= 'ticker')
                    dfQuartil['retorno'] = dfQuartil['preco_fechamento_ajustado_posterior'] / dfQuartil['preco_fechamento_ajustado'] - 1
                    retornoQuartil = dfQuartil['retorno'].mean()
                    listaRetornos.append(retornoQuartil)
                    dfPremios.loc[data, colunas[zeta]] = retornoQuartil
                
                dfPremios.loc[data, 'universo'] = np.mean(np.array(listaRetornos))

            dfInfoPontuais['ranking_final'] = 0

            for alfa, indicador in enumerate(self.dfIndicadores):

                indicadorNaData = indicador.loc[indicador['data'] == data, ['ticker', 'valor']].dropna()

                indicadorNaData.columns = ['ticker',f'indicador_peg_ratio']
                dfInfoPontuais = pd.merge(dfInfoPontuais, indicadorNaData, how='inner', on='ticker')

            dfInfoPontuais['comeco_ticker'] = dfInfoPontuais['ticker'].astype(str).str[0:4]
            dfInfoPontuais.sort_values('volume_negociado', ascending= False, inplace= True)
            dfInfoPontuais.drop_duplicates('comeco_ticker', inplace= True)
            dfInfoPontuais.drop('comeco_ticker', axis=1, inplace= True)

            dfInfoPontuais['ranking_0'] = dfInfoPontuais['indicador_peg_ratio'].rank(ascending=True)
            dfInfoPontuais.sort_values('ranking_final', ascending=True, inplace=True)

            empresasPorQuartil = len(dfInfoPontuais) // 4
            sobraEmpresas = len(dfInfoPontuais) % 4

            listaDfs[0] = dfInfoPontuais.iloc[0: empresasPorQuartil]
            listaDfs[1] = dfInfoPontuais.iloc[empresasPorQuartil: (empresasPorQuartil * 2)]
            listaDfs[2] = dfInfoPontuais.iloc[(empresasPorQuartil * 2):(empresasPorQuartil * 3)]
            listaDfs[3] = dfInfoPontuais.iloc[(empresasPorQuartil * 3):((empresasPorQuartil * 4) + sobraEmpresas)]
        
        dfPremios['nome_premio'] = self.nomePremio
        dfPremios['liquidez'] = self.liquidez
        dfPremios.reset_index(names='data', inplace=True)
        dfPremios['id_premio'] = dfPremios['nome_premio'].astype(str) + '_' + dfPremios['liquidez'].astype(str) + '_' + dfPremios['data'].astype(str)
        dfPremios.dropna(inplace= True)
        self.dfPremios = dfPremios

    def colocando_premio_na_base(self):

        self.dfPremios.to_parquet(f'{self.caminhoSalvarArquivo}/{self.nomePremio}_{self.liquidez}.parquet', index= False)

if __name__ == '__main__':

    premio = PremioRisco(liquidez= 1000000, nomePremio= 'VALOR_PEG_RATIO', caminhoDados=r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\dados', caminhoSalvarArquivo= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\premios_risco')

    premio.pegando_dados_cotacoes()
    premio.pegando_datas_possiveis()
    premio.filtrando_volume()
    premio.descobrindo_mes_inicial()
    premio.calculando_premio()
    premio.colocando_premio_na_base()