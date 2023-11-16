import pandas as pd

class MarketPremium():

    def __init__(self, caminhoSalvarArquivo = '', caminhoDados = ''):

        self.caminhoSalvarArquivos = caminhoSalvarArquivo
        self.caminhoDados = caminhoDados

    def calculando_premio(self):

        cdi = pd.read_parquet(f'{self.caminhoDados}/cdi.parquet')
        cdi['cota'] = (1 + cdi['retorno']).cumprod() - 1
        ibov = pd.read_parquet(f'{self.caminhoDados}/ibov.parquet')

        ibovDatas = ibov.sort_values('data', ascending= True)
        ibovDatas = ibovDatas.assign(year = pd.DatetimeIndex(ibovDatas['data']).year)
        ibovDatas = ibovDatas.assign(month = pd.DatetimeIndex(ibovDatas['data']).month)
        datasFinalMes = ibovDatas.groupby(['year','month'])['data'].last()
        diasFinaldeMes = datasFinalMes.to_list()

        ibov = ibov[ibov['data'].isin(diasFinaldeMes)]
        cdi = cdi[cdi['data'].isin(diasFinaldeMes)]
        ibov['retorno_ibov'] = ibov['fechamento'].pct_change()
        cdi['retorno_cdi'] = cdi['cota'].pct_change()
        ibov['data'] = ibov['data'].astype(str)
        cdi['data'] = cdi['data'].astype(str)

        dfDadosMercado = pd.merge(ibov, cdi, how= 'inner', on= 'data')
        dfDadosMercado['mkt_premium'] = (1 + dfDadosMercado['retorno_ibov'])/(1 + dfDadosMercado['retorno_cdi']) - 1
        dfDadosMercado = dfDadosMercado.dropna()
        dfDadosMercado = dfDadosMercado[['data', 'mkt_premium']]
        dfDadosMercado['data'] = pd.to_datetime(dfDadosMercado['data']).dt.date

        dfDadosMercado.to_parquet(f'{self.caminhoSalvarArquivos}/market_premium.parquet', index= False)


if __name__ == '__main__':

    beta = MarketPremium(caminhoSalvarArquivo= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\premios_risco', caminhoDados= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\dados')

    beta.calculando_premio()