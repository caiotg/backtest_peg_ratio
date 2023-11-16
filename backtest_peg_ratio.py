import pandas as pd
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
import os
from resultados import ReportResult


class Backtest():

    def __init__(self, dataFinal, filtroLiquidez, balanceamento, numeroAtivos, corretagem = 0.01, dataInicial = None, caminhoDados = None):

        try:

            dataInicial = dt.datetime.strptime(dataInicial, '%Y-%m-%d').date()
            dataFinal = dt.datetime.strptime(dataFinal, '%Y-%m-%d').date()

        except:

            dataFinal = dt.datetime.strptime(dataFinal, '%Y-%m-%d').date()


        self.dataFinal = dataFinal
        self.dataInicial = dataInicial
        self.filtroLiquidez = filtroLiquidez
        self.balanceamento = balanceamento
        self.numeroAtivos = numeroAtivos
        self.corretagem = corretagem

        os.chdir(caminhoDados)

    def pegando_dados(self):

        listaDfs = []

        cotacoes = pd.read_parquet('cotacoes.parquet')
        cotacoes['data'] = pd.to_datetime(cotacoes['data']).dt.date
        cotacoes['ticker'] = cotacoes['ticker'].astype(str)
        self.cotacoes = cotacoes.sort_values('data', ascending= True)

        volumeMediano = pd.read_parquet('volume_mediano.parquet')
        volumeMediano['data'] = pd.to_datetime(volumeMediano['data']).dt.date
        volumeMediano['ticker'] = volumeMediano['ticker'].astype(str)
        volumeMediano = volumeMediano[['data','ticker','valor']]
        volumeMediano.columns = ['data','ticker','volume']

        pegRatio = pd.read_parquet('peg_ratio.parquet')
        pegRatio['data'] = pd.to_datetime(pegRatio['data']).dt.date
        pegRatio['ticker'] = pegRatio['ticker'].astype(str)
        pegRatio['valor'] = pegRatio['valor'].astype(float)
        pegRatio = pegRatio[['data','ticker','valor']]
        pegRatio.columns = ['data','ticker','peg_ratio']

        listaDfs.append(self.cotacoes)
        listaDfs.append(volumeMediano)
        listaDfs.append(pegRatio)

        dfDados = listaDfs[0]

        for df in listaDfs[1:]:

            dfDados = pd.merge(dfDados, df, how='inner', left_on=['data','ticker'], right_on=['data','ticker'])

        self.dfDados = dfDados.dropna()

    def filtrando_datas(self):

        dfDados = self.dfDados

        if self.dataInicial != None:

            dfDados = dfDados[dfDados['data'] >= self.dataInicial]

        else:

            dfDados = dfDados[dfDados['data'] >= (min(dfDados['data']) + relativedelta(months=+2))]

        dfDados = dfDados[dfDados['data'] < self.dataFinal]

        datasDisponiveis = np.sort(dfDados['data'].unique())
        periodoDeDias = [datasDisponiveis[i] for i in range(0, len(datasDisponiveis), self.balanceamento)]

        dfDados = dfDados[dfDados['data'].isin(periodoDeDias)]

        self.dfDados = dfDados

    def criando_carteiras(self):

        dfDados = self.dfDados

        dfDados = dfDados[dfDados['volume'] > self.filtroLiquidez]

        dfDados = dfDados.assign(TICKER_PREFIX = dfDados['ticker'].str[:4])
        dfDados = dfDados.loc[dfDados.groupby(['data','TICKER_PREFIX'])['volume'].idxmax()]
        dfDados = dfDados.drop('TICKER_PREFIX', axis= 1)

        dfCarteiras = dfDados.copy()

        dfCarteiras['PEG_RATIO_RANK'] = dfCarteiras.groupby('data')['peg_ratio'].rank(ascending= True)

        dfCarteiras['posicao_carteira'] = dfCarteiras.groupby('data')['PEG_RATIO_RANK'].rank()
        portifolio = dfCarteiras[dfCarteiras['posicao_carteira'] <= self.numeroAtivos]
        portifolio = portifolio.assign(peso = 1/(portifolio.groupby('data').transform('size')))

        carteiraPorPeriodo = portifolio
        carteiraPorPeriodo = carteiraPorPeriodo.sort_values('data', ascending= True)[['data','ticker','peso']]
        carteiraPorPeriodo = carteiraPorPeriodo.groupby(['data','ticker'])['peso'].sum()

        self.carteiraPorPeriodo = carteiraPorPeriodo.reset_index()

    def calculando_retorno(self):

        cotacoes = self.cotacoes[(self.cotacoes['data'] >= self.carteiraPorPeriodo.iloc[0,0]) & (self.cotacoes['data'] <= self.dataFinal)]
        
        datasCarteira = cotacoes['data'].unique()

        dfRetornos = pd.DataFrame(columns= ['data','dinheiro','numero_trade'], index= list(range(0, len(datasCarteira))))

        carteira = 0
        dinheiroInicial = 10000

        dfRetornos.iloc[1, 0] = self.carteiraPorPeriodo.iloc[1,0]
        dfRetornos.iloc[1, 1] = dinheiroInicial
        dfRetornos.iloc[1,2] = carteira

        cotacoes = cotacoes.assign(var_fin = cotacoes.groupby('ticker')['preco_fechamento_ajustado'].diff())

        retornoFin = cotacoes[['data','ticker','var_fin']]

        carteiras = self.carteiraPorPeriodo.copy()
        datasRebalanceamento = carteiras['data'].unique()

        cotacoesRebalanceamento = cotacoes[['ticker','data','preco_fechamento_ajustado']]

        retornoFin.set_index(['data','ticker'], inplace= True)
        carteiras.set_index(['data','ticker'], inplace= True)
        cotacoesRebalanceamento.set_index(['data','ticker'], inplace= True)

        for i, data in enumerate(datasCarteira):

            if i not in [0,1]:
                
                retornoFinDia = retornoFin.loc[data]

                varPatrimonioDia = (carteiraVigente['quantidade_acoes'] * retornoFinDia['var_fin']).sum()
                dfRetornos.iloc[i, 0] = data
                dfRetornos.iloc[i, 1] = dfRetornos.iloc[i-1, 1]
                dfRetornos.iloc[i, 1] += varPatrimonioDia
                dfRetornos.iloc[i, 2] = carteira

            if data in datasRebalanceamento:

                carteiraNaData = carteiras.loc[data].copy()
                trocarCarteira = True
                delay = 0

            if trocarCarteira:

                if delay == 0:

                    delay = delay + 1

                else:

                    carteiraNaData['dinheiro_por_acao'] = (carteiraNaData['peso'] * dfRetornos.iloc[i, 1] * (1 - self.corretagem))
                    cotacoesNaData = cotacoesRebalanceamento.loc[data]
                    carteiraVigente = pd.merge(carteiraNaData, cotacoesNaData, left_index=True, right_index= True)
                    carteiraVigente['quantidade_acoes'] = carteiraVigente['dinheiro_por_acao'] / carteiraVigente['preco_fechamento_ajustado']
                    carteira += 1
                    trocarCarteira = False

        dfRetornos = dfRetornos.assign(retorno = dfRetornos['dinheiro'].pct_change())
        dfRetornos = dfRetornos.drop(0, axis=0)

        self.dfRetornos = dfRetornos

    def make_report(self):

        self.carteiraPeriodos = self.carteiraPorPeriodo.set_index('data')

        ReportResult(dfTrades= self.dfRetornos, dfCarteiras= self.carteiraPeriodos, caminhoImagens=r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\imagens')


if __name__ == '__main__':

    backtest = Backtest(dataFinal= '2023-12-31', dataInicial= '2011-12-23', balanceamento= 21, numeroAtivos= 10, filtroLiquidez= 1000000, caminhoDados= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\dados')

    backtest.pegando_dados()
    backtest.filtrando_datas()
    backtest.criando_carteiras()
    backtest.calculando_retorno()
    backtest.make_report()

    print(backtest.carteiraPorPeriodo.tail(20))
  