"""
OLS.py mainly trains OLS model and completes the OLS tests.
Before do the training, add word_num_log, question lengh, the proportion of total question's topics/labels and topic heat into data.
Then MinmaxScaler and Logarithm are done to the independent variables.

def OLS_train gets the OLS result and you can use the .summary() to see it.
def Pearson calculates the Pearson coefficient.
def multicollinearity_test is for multicollinearity test based on VIF
def normality_test uses K-S test for normality test
def heteroscedasticity_test uses White and BC test for heteroscedasticity test
"""
import pickle
import pandas as pd
import numpy as np
import conj
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from patsy.highlevel import dmatrices
import scipy.stats as stats


def OLS_train(endog, exdog, constant=True):
    """
    The def give the OLS training
    :param endog: The dependent variable. DataFrame
    :param exdog: The independent variable. DataFrame
    :param constant: Whether add intercept to model. Boolean
    :return: est: The reult of OLS. fit()
    """
    if constant:
        exdog = sm.add_constant(exdog)
    est = sm.OLS(endog, exdog).fit()
    return est


def Pearson(endog, exdog):
    """
    The def calculates the Pearson coefficient
    :param endog: The dependent variable. DataFrame
    :param exdog: The independent variable. Series
    :return: pearson; Pearson coefficient
    """
    pearson = exdog.corrwith(endog)
    return pearson


def multicollinearity_test(endog, exdog, data):
    """
    The def uses VIF-factor to do multicollinearity test
    :param endog: The dependent variable. String
    :param exdog: The independent variable. String, different variables are connected by '+'
    :param data: The data set. DataFrame
    :return: vif: VIF-factor. DataFrame
    """
    y, x = dmatrices(endog+'~'+exdog, data=data, return_type='dataframe')
    vif = pd.DataFrame()
    vif['VIF Factor'] = [variance_inflation_factor(x.values, i) for i in range(x.shape[1])]
    vif['features'] = x.columns
    return vif


def normality_test(ols_result):
    """
    The def uses K-S test to do normality test.
    :param ols_result: The OLS fit result. fit()
    :return: ks: K-S test result. List
    """
    resid = ols_result.resid
    standard_resid = (resid-np.mean(resid))/np.std(resid)
    ks = stats.kstest(standard_resid, 'norm')
    return ks


def heteroscedasticity_test(endog, exdog, data):
    """
    The def uses White test & Breus-Chpagan test to do heteroscedasticity test.
    :param endog: The dependent variable. String
    :param exdog: The independent variable. String, different variables are connected by '+'
    :param data: The data set. DataFrame
    :return: white: White test pvalue
             bc: Breus-Chpagan test pvalue
    """
    ols_result = sm.formula.ols(
        endog + '~' + exdog,
        data=data).fit()
    white = sm.stats.diagnostic.het_white(ols_result.resid, exog=ols_result.model.exog)[1]
    bc = sm.stats.diagnostic.het_breuschpagan(ols_result.resid, exog_het=ols_result.model.exog)[1]
    return white, bc





def write_result(file_name, data):
    with open(file_name, 'a') as f:
        f.write(data)


def train(data, file_dir):
    # write_result(file_dir, 'The topic is '+topic)
    # dta = prepare_data[prepare_data['topic'] == topic]
    dta = data.convert_objects(convert_numeric=True)
    X = dta[['word_num_log_log', 'word_num_log', 'ask_price_log', 'answer_urank_log', 'gen_dis_log', 'asker_urank_log','gen_dis_log', 'verb_count_log', 'adj_count_log']]
    ols = OLS_train(dta[['onlooker_count_log']], X, constant=True)
    print(ols.summary())  # give the OLS results
    write_result(file_dir, str(ols.summary()))
    y = dta.onlooker_count_log
    print('\nPearson: \n')
    write_result(file_dir, '\nPearson: \n')
    print(Pearson(y, X))  # give Pearson coefficient
    write_result(file_dir, str(Pearson(y, X)))
    print('\nVIF: \n')
    write_result(file_dir, '\nVIF: \n')
    print(multicollinearity_test('onlooker_count_log', 'word_num_log_log+word_num_log+ask_price_log+'
                                                       'gen_dis_log+answer_urank_log+asker_urank_log+gen_dis_log+verb_count_log+adj_count_log',
          dta)
          )  # give multicollinearity test result
    write_result(file_dir, str(multicollinearity_test('onlooker_count_log', 'word_num_log_log+word_num_log+ask_price_log+'
                                                       'gen_dis_log+answer_urank_log+asker_urank_log+gen_dis_log+verb_count_log+adj_count_log',
          dta)))
    print('\nK-S(pay attention to pvalue): \n')
    print(normality_test(ols))  # give normality test result
    write_result(file_dir, '\nK-S(pay attention to pvalue): \n')
    write_result(file_dir, str(normality_test(ols)))
    print('\nWhite & BC: \n')
    print(heteroscedasticity_test('onlooker_count_log', 'word_num_log_log+word_num_log+ask_price_log+'
                                                       'gen_dis_log+answer_urank_log+asker_urank_log+gen_dis_log+verb_count_log+adj_count_log',
          dta))  # give heteroscedasticity test
    write_result(file_dir, '\nWhite & BC: \n')
    write_result(file_dir, str(heteroscedasticity_test('onlooker_count_log', 'word_num_log_log+word_num_log+ask_price_log+'
                                                       'gen_dis_log+answer_urank_log+asker_urank_log+gen_dis_log+verb_count_log+adj_count_log',
          dta)))


filesolver = conj.FileSolve()
# prepare_data = filesolver.read_pkl('../qa_txt/prepare_dta8.pkl')
prepare_data = pd.read_excel('../qa_txt/unmark_dta.xlsx')
# prepare_data = prepare_data[prepare_data['mark'] == 0]
prepare_data.dropna(inplace=True)
prepare_data = prepare_data.reset_index(drop=True)
prepare_data['mark_distance'] = prepare_data['mark']*prepare_data['gen_dis_log']
dta = prepare_data.convert_objects(convert_numeric=True)
print(dta.info())
file_dir = '../qa_txt/011101.txt'
X = dta[['ask_price_log','word_num_log','gen_dis_log','answer_followers_count_log','asker_followers_count_log','asker_urank_log','answer_urank_log','answer_verified_type','asker_verified_type']]
ols = OLS_train(dta[['onlooker_count_log']], X, constant=True)
print(ols.summary())  # give the OLS results
write_result(file_dir, str(ols.summary()))
y = dta.onlooker_count_log
print('\nPearson: \n')
write_result(file_dir, '\nPearson: \n')
print(Pearson(y, X))  # give Pearson coefficient
write_result(file_dir, str(Pearson(y, X)))
print('\nVIF: \n')
write_result(file_dir, '\nVIF: \n')
print(multicollinearity_test('onlooker_count_log', 'ask_price_log+word_num_log+gen_dis_log+answer_followers_count_log+asker_followers_count_log+asker_urank_log+answer_urank_log+answer_verified_type+asker_verified_type',
      dta)
      )  # give multicollinearity test result
write_result(file_dir, str(multicollinearity_test('onlooker_count_log', 'ask_price_log+word_num_log+gen_dis_log+answer_followers_count_log+asker_followers_count_log+asker_urank_log+answer_urank_log+answer_verified_type+asker_verified_type',
      dta)))
print('\nK-S(pay attention to pvalue): \n')
print(normality_test(ols))  # give normality test result
write_result(file_dir, '\nK-S(pay attention to pvalue): \n')
write_result(file_dir, str(normality_test(ols)))
print('\nWhite & BC: \n')
print(heteroscedasticity_test('onlooker_count_log', 'ask_price_log+word_num_log+gen_dis_log+answer_followers_count_log+asker_followers_count_log+asker_urank_log+answer_urank_log+answer_verified_type+asker_verified_type',
      dta))  # give heteroscedasticity test
write_result(file_dir, '\nWhite & BC: \n')
write_result(file_dir, str(heteroscedasticity_test('onlooker_count_log', 'ask_price_log+word_num_log+gen_dis_log+answer_followers_count_log+asker_followers_count_log+asker_urank_log+answer_urank_log+answer_verified_type+asker_verified_type',
      dta)))

