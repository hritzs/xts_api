import numpy as np
from scipy.stats import norm
import math
import warnings
# import numba
# from numba.errors import mark_try_block
warnings.simplefilter('ignore')


# @numba.jit(nopython=True)
def blackScholes(calculation_type, Option_type, K, S, T, sigma, r):
    
    
    K = float(K)
    S = float(S)
    T = float(T)
    T = T/365
    calculation_type = calculation_type.lower()
    Option_type = Option_type.lower()
    #PRICE
    
    
    if math.isnan(sigma) or sigma == 0: 
        print(f'sigma: {sigma}' )
        return float('NaN')
    
    if math.isnan(K) or K == 0: 
        print(f'K: {K}' )
        return float('NaN')
    
    if math.isnan(S) or S == 0: 
        print(f'S: {S}' )
        return float('NaN')
   
    if math.isnan(T) or T == 0: 
        print(f'T: {T}' )
        return float('NaN')
    
    if calculation_type=="p":
        
        
        d1 = (np.log(S/K) + (r + sigma**2/2)*T)/(sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        try:
            if Option_type == "c":
                price = S*norm.cdf(d1, 0, 1) - K*np.exp(-r*T)*norm.cdf(d2, 0, 1)
            elif Option_type == "p":
                price = K*np.exp(-r*T)*norm.cdf(-d2, 0, 1) - S*norm.cdf(-d1, 0, 1)
            return price
        except:
            print("Please confirm option type, either 'c' for Call or 'p' for Put!")
            
    
    #DELTA
    elif calculation_type=="d":
        d1 = (np.log(S/K) + (r + sigma**2/2)*T)/(sigma*np.sqrt(T))
        try:
            if Option_type == "c":
                delta_calc = norm.cdf(d1, 0, 1)
            elif Option_type == "p":
                delta_calc = -norm.cdf(-d1, 0, 1)
            return delta_calc
        except:
            print("Please confirm option type, either 'c' for Call or 'p' for Put!")
            
    #GAMMA
    elif calculation_type=="g":
        d1 = (np.log(S/K) + (r + sigma**2/2)*T)/(sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        try:
            gamma_calc = norm.pdf(d1, 0, 1)/(S*sigma*np.sqrt(T))
            return gamma_calc
        except:
            print("Please confirm option type, either 'c' for Call or 'p' for Put!")
    
    #VEGA
    elif calculation_type=="v":
        d1 = (np.log(S/K) + (r + sigma**2/2)*T)/(sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        try:
            vega_calc = S*norm.pdf(d1, 0, 1)*np.sqrt(T)
            return vega_calc*0.01
        except:
            print("Please confirm option type, either 'c' for Call or 'p' for Put!")
    
    
    #THETA
    elif calculation_type=="t":
        d1 = (np.log(S/K) + (r + sigma**2/2)*T)/(sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        try:
            if Option_type == "c":
                theta_calc = -S*norm.pdf(d1, 0, 1)*sigma/(2*np.sqrt(T)) - r*K*np.exp(-r*T)*norm.cdf(d2, 0, 1)
            elif Option_type == "p":
                theta_calc = -S*norm.pdf(d1, 0, 1)*sigma/(2*np.sqrt(T)) + r*K*np.exp(-r*T)*norm.cdf(-d2, 0, 1)
            return theta_calc/365
        except:
            print("Please confirm option type, either 'c' for Call or 'p' for Put!")


    #RHO
    elif calculation_type=="r":
        d1 = (np.log(S/K) + (r + sigma**2/2)*T)/(sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        try:
            if Option_type == "c":
                rho_calc = K*T*np.exp(-r*T)*norm.cdf(d2, 0, 1)
            elif Option_type == "p":
                rho_calc = -K*T*np.exp(-r*T)*norm.cdf(-d2, 0, 1)
            return rho_calc*0.01
        except:
            print("Please confirm option type, either 'c' for Call or 'p' for Put!")


# @numba.jit(nopython=True)
def implied_volatility(Option_type, K, S, T, Option_price, r=0, tol=0.0001, max_iterations=100):
    # Option_type = 'c'
    # K = 750
    # S = 1000
    # T = 1
    # Option_price = 1000
    
    
    # print("Option_type: "+str(Option_type))
    # print("STrike::"+str(K))
    # print("Spot:"+str(S))
    # print("DTE"+str(T))
    # print("OPtion Price:"+str(Option_price))
    # print("r:"+str(r))
    
    K = float(K)
    S = float(S)
    T = float(T)
    
    if math.isnan(Option_price) or Option_price == 0: 
        # print(f'Option_price: {Option_price}' )
        return float('NaN')
    
    if math.isnan(float(K)) or K == 0:
        print(f'Strike Price: {K}')
        return float('NaN')
    
    if math.isnan(float(S)) or S == 0:
        print(f'UnderlyingSpot: {S}')
        return float('NaN')
    
    if math.isnan(float(T)) or T == 0:
        print(f'DTE: {T}')
        return float('NaN')
    
   
    
    Option_type = Option_type.lower()
    
    if Option_type == 'c':
        intrinsic = max(S - K, 0)
    else:
        intrinsic = max(K - S, 0)
    
    if Option_price <= intrinsic:
        return 0
    # T = T
    N_prime = norm.pdf
    N = norm.cdf
    # print(N)
    
    
    if ((math.sqrt(2*math.pi)/math.sqrt(T/365))*(Option_price/S) <= 0.03) :
        sigma = 0.2
    elif (math.sqrt(2*math.pi)/math.sqrt(T/365))*(Option_price/S) >= 4 :
        sigma = 2.5
    else:
        sigma = (math.sqrt(2*math.pi)/math.sqrt(T/365))*(Option_price/S)
    
    # print('Sigma',sigma)
    for i in range(max_iterations):
        # print(i)
        if(Option_type == "c"):
            # print(S,K,T,sigma)
            if blackScholes("p", "c", K, S, T, sigma, r)<0.05:
                while(blackScholes("p", "c", K, S, T, sigma, r)<0.05):
                    sigma+=0.1
                    # print(sigma)
            
            diff = blackScholes("p", "c", K, S, T, sigma, r) - Option_price
            # print('Diff',diff)
            # print(diff)
            if abs(diff) < tol:
                # print(f'found on {i}th iteration')
                # print(f'difference is equal to {diff}')
                break
            
            sigma = sigma - (diff / blackScholes("v", "c", K, S, T, sigma, r))/100
            if sigma > 4:
                sigma = 4
            # print('Sigma2',sigma)
            # print(sigma)
        
        else:
            if blackScholes("p", "p", K, S, T, sigma, r)<0.05:
                while(blackScholes("p", "p", K, S, T, sigma, r)<0.05):
                    sigma+=0.01
            diff = blackScholes("p", "p", K, S, T, sigma, r) - Option_price
            # print('diff',diff)
            # print(S,K,T,sigma)
            if abs(diff) < tol:
                # print(f'found on {i}th iteration')
                # print(f'difference is equal to {diff}')
                break

            sigma = sigma - (diff / blackScholes("v", "p", K, S, T, sigma, r))/100
            if sigma > 4:
                sigma = 4
            # print('Sigma3',sigma)
            # print(sigma)
        # if math.isnan(sigma):
        #     return (math.sqrt(2*math.pi)/math.sqrt(T/365))*(Option_price/S)
        # print (sigma)
    # print('Sigma',sigma)
    return sigma

        
# callprice = blackScholes("p", "c", 100, 100, 15, 0.1855, 0)
# putprice = blackScholes("p", "p", 1350, 1400, 15, 0.25, 0)
# callDelta = blackScholes("d", "c", 1350, 1400, 15, 0.25, 0)
# callgamma = blackScholes("g", "c", 1350, 1400, 15, 0.25, 0)
# callvega = blackScholes("v", "c", 1350, 1400, 15, 0.25, 0)
# calltheta = blackScholes("t", "c", 1350, 1400, 15, 0.25, 0)
# callrho = blackScholes("r", "c", 1350, 1400, 15, 0.25, 0)

# callIV = implied_volatility("c", 42200, 41653.55, 1, 14.05, 0)
# putIV = implied_volatility("p", 102, 100, 15, 1.5, 0)

# print(callIV)
# # print(putIV)



# Option_type = "c"
# K = 102
# S = 100
# T = 15
# Option_price =1.5
# r = 0
# tol=0.0001
# max_iterations=1000

# print("callPrice: ", callprice)
# print("putPrice: ", putprice)
# print("callDelta: ", callDelta)
# print("callgamma: ", callgamma)
# print("callvega: ", callvega)
# print("calltheta: ", calltheta)
# print("callrho: ", callrho)
# print("callIV: ", callIV)
# print("putIV: ", putIV)

