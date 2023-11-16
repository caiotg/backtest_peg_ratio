import pandas as pd
import statsmodels.api as sm

class linear_regression():

    def __init__(self, dataFinalAnalise, dicionarioFatores ,caminhoPremiosDeRisco = '.', caminhoCdi = '.'):

        self.caminhoPremiosDeRisco = caminhoPremiosDeRisco
        self.caminhoCdi = caminhoCdi
        self.dicionarioFatores = dicionarioFatores
        self.dataFinalAnalise = dataFinalAnalise

        self.listaNomeFatores = []
        self.liquidez = []

        for key, item in dicionarioFatores.items():

            self.listaNomeFatores.append(key)
            self.liquidez.append(item)


    def puxando_dados_premios(self):

        dfPremios = pd.read_parquet(f'{self.caminhoPremiosDeRisco}/market_premium.parquet')
        dfPremios['data'] = pd.to_datetime(dfPremios['data'])

        for i, nomePremio in enumerate(self.listaNomeFatores):

            df = pd.read_parquet(f'{self.caminhoPremiosDeRisco}/{nomePremio}_{self.liquidez[i]}.parquet')
            df['data'] = pd.to_datetime(df['data'])

            df = df.assign(premio_fator = (1 + df['primeiro_quartil'])/(1 + df['quarto_quartil']) - 1)
            universoFator = df[['data', 'universo']]
            universoFator.columns = ['data', f'universo_{nomePremio}']

            if i == 0:

                universo = universoFator
            
            else:

                universo = pd.merge(universo, universoFator, on='data')

            df = df[['data', 'premio_fator']]
            df.columns = ['data', nomePremio]

            dfPremios = pd.merge(df, dfPremios, how= 'inner', on= 'data')

        dfPremios = dfPremios.drop(dfPremios.index[0], axis= 0)
        dfPremios = dfPremios.set_index('data')
        dfPremios = dfPremios[dfPremios.index < self.dataFinalAnalise]
        universo = universo[universo['data'] < self.dataFinalAnalise]

        self.dfPremios = dfPremios
        self.universo = universo

    def calculando_universo(self):

        universo = self.universo
        universo = universo.set_index('data')
        universo['universo_medio'] = universo.mean(axis= 1)
        universo = universo.reset_index()
        universo = universo[['data', 'universo_medio']]

        cdi = pd.read_parquet(f'{self.caminhoCdi}/cdi.parquet')
        cdi['data'] = pd.to_datetime(cdi['data'])
        cdi['cota'] = (1 + cdi['retorno']).cumprod() -1
        cdi = cdi[cdi['data'].isin(universo['data'].to_list())]
        cdi['rf'] = cdi['cota'].pct_change()
        cdi = cdi.dropna()
        cdi = cdi[['data', 'rf']]

        universo = pd.merge(universo, cdi, how= 'inner', on= 'data')
        universo['U_RF'] = (1 + universo['universo_medio'])/ (1 + universo['rf']) - 1
        universo = universo.set_index('data')
        self.universo = universo

    def regressao(self):

        Y = self.universo['U_RF']
        X = self.dfPremios

        X_C = sm.add_constant(X)

        model = sm.OLS(Y, X_C)
        resultado = model.fit()
        print(resultado.summary())

if __name__ == '__main__':

    dicionarioFatores = {
        'VALOR_PEG_RATIO': 1000000,
    }


    fazendoModelo = linear_regression(dataFinalAnalise= '2020-12-31', dicionarioFatores= dicionarioFatores, caminhoPremiosDeRisco= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\premios_risco', caminhoCdi= r'C:\Users\Caio\Documents\dev\github\backtest_peg_ratio\dados')

    fazendoModelo.puxando_dados_premios()
    fazendoModelo.calculando_universo()
    fazendoModelo.regressao()
