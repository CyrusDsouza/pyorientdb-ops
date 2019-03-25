# -*- coding: utf-8 -*-
"""
Created on Wed May 16 12:44:42 2018
@author: CYRUS DSOUZA
"""

#----------------------------------------------------------------------------------#

import traceback
import random
import sys
import re
import string
import itertools
import time 


import globs as gb

#----------------------------------------------------------------------------------#

def id_generator(size = 10):
    return "".join(random.choice('0123456789ABCDEF') for i in range(size))

#----------------------------------------------------------------------------------#

def find_ngrams(input_list, n):
  return zip(*[input_list[i:] for i in range(n)])

#----------------------------------------------------------------------------------#  


def peek(iterable):
    #used to iterate through generators
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return first, itertools.chain([first], iterable)

def resolve_symbols(string):
        
    if not string.strip():
        return string
    
    
    name = ""
    tokens = list(string.split())
    
    for token in tokens:
        
        if gb.DECIMAL_DELIM in token:
            token = token.replace(gb.DECIMAL_DELIM,".")
    
        if gb.LESSTHAN_DELIM in string:
            token = token.replace(gb.LESSTHAN_DELIM,"<")                    
        
        if gb.GREATERTHAN_DELIM in string:
            token = token.replace(gb.GREATERTHAN_DELIM,">")                            
    
        if gb.PERCENTAGE_DELIM in string:
            token = token.replace(gb.PERCENTAGE_DELIM,"%")
            
        if gb.DOLLAR_DELIM in string:
            token = token.replace(gb.DOLLAR_DELIM,"$") 
        
        
        name += token + " "
        
    return name.strip()

def odb_number_formatter(string, from_name_formatter= False):
    """
    Check whether a string is a number, decimal, greaterthan, lessthan, percentage 
    and other symbols 
    
    """
    if not string.strip():
        return string


    tokens = list(string.split())
    
    
    #Check if the end of the string is a decimal vs punctuation
    
    
    name = ""
    
    if "." in tokens[-1]:
        if list(tokens[-1])[-1] == '.':
            tokens[-1] = tokens[-1].replace(".","")
        
    for token in tokens: 
        
        character_token = list(token)
        
        if "." in character_token:
            token = token.replace(".",gb.DECIMAL_DELIM)
            
        if "<" in character_token:
            token = token.replace("<",gb.LESSTHAN_DELIM)
            
        if ">" in character_token:
            token =  token.replace(">",gb.GREATERTHAN_DELIM)
            
        if "%" in character_token: 
            token = token.replace("%", gb.PERCENTAGE_DELIM)
            
        if "$" in character_token:
            token = token.replace("$", gb.DOLLAR_DELIM)
            
        if "," in character_token:
            comma = character_token.index(",")
            if comma == len(character_token)-1:
                token = token.replace(",","")
                
                            
            elif character_token[comma-1].isdigit() and \
                            character_token[comma+1].isdigit():
                token = token.replace(",", "")
            
        
        name += token + " "
    
    return name.strip()
    
def odb_name_formatter(name, sentence = False):
    """
    OrientDB requires class names to be only character based and does not allow
    the class to start with the number as well as it should not have spaces in them  
    
    
    >>> odb_name_formatter("2017 ABC plan")
    >>> '#_2017'_ABC_plan'
    
    >>> odb_name_formatter("ABC plan plc")
    >>> "ABC_plan_plc"
    
    
    """    
    
    original_name = name
    
    refined_order = {}
        
    #check for spaces in the string
     
    if " "  in name:
        newname = odb_number_formatter(name, from_name_formatter = True)
        try:
            name = newname.translate(string.punctuation)
        except Exception as e:
            name = newname
            # print e     
        name = "_".join(name.split())
    
    
    else:
        name = odb_number_formatter(name)

    
    #check if a number exists in the string        
    if any(char.isdigit() for char in name):
        dig_find = re.compile(r"[\d]+")
        word_find = re.compile(r"[A-Za-z]+")
        space_find  = re.compile(r"[_]")


    
        for dig_matcher in dig_find.finditer(name):
            dm,dg = dig_matcher.start(), dig_matcher.group()
            refined_order[dm] = dg
        
        for word_matcher in word_find.finditer(name):
            wm,wg = word_matcher.start(), word_matcher.group()            
            refined_order[wm] = wg   
            
        for space_matcher in space_find.finditer(name):
            sm, sg = space_matcher.start(), space_matcher.group()
            refined_order[sm] = sg

        #check if the first element is a number
        if 0 in refined_order and refined_order[0].isdigit():
            refined_order[0] = gb.CLASSNAME_DELIM + refined_order[0]
            
        #sort the list to get the order of the string    
        refined_order_list = sorted(refined_order.iteritems())
        
        
        final_refined_order_list = {}
        skip_list = []




        #To make sure we dont separate numbers within words like 'EC4'
        #Earlier this used to be separated like ec_4 instead of ec4
        
        for i in range(len(refined_order_list)):
            if i == len(refined_order_list)-1:
                
                if refined_order_list[i] in skip_list:
                    continue
                
                if refined_order_list[i][1].isdigit():
                    if sentence:
                        old_value = refined_order_list[i][1]
                        final_refined_order_list[refined_order_list[i][0]] = old_value
                    else:
                        break
                        
                
                old_value = refined_order_list[i][1]
                final_refined_order_list[refined_order_list[i][0]] = old_value
                break
                
            if refined_order_list[i] in skip_list:
                continue
            
            if refined_order_list[i][1] == "_":
                continue
            

            
            if refined_order_list[i+1][1] != "_":
                new_value = refined_order_list[i][1] + refined_order_list[i+1][1]
                final_refined_order_list[refined_order_list[i][0]] = new_value
                skip_list.append(refined_order_list[i+1])
            else:
                old_value = refined_order_list[i][1]
                final_refined_order_list[refined_order_list[i][0]] = old_value
            
            
        final_refined_order_list = sorted(final_refined_order_list.iteritems())
            

        refined_name = "_".join([a[1].lower() for a in final_refined_order_list if a[1] != "_"])
    
    else:
        refined_name = name

        

    return refined_name

            
#----------------------------------------------------------------------------------#
    
def timecheck(method):
    """Check the time it takes to execute a function"""
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print '%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000)
        return result
    return timed

#----------------------------------------------------------------------------------#
    
def errorcheck(method):
    """Check the errors for custom functions"""
    def check(*args, **kw):
        try:
            result = method(*args, **kw)
            return result
        except Exception as e:
            print(format_exception(e))
            print("Error occured in '{}' with error - [{}].\n Check Traceback for further details : -->".format(method.__name__,e))
            print(format_exception(e))
#
    return check

#----------------------------------------------------------------------------------#
    
def format_exception(e):
    exception_list = traceback.format_stack()
    exception_list = exception_list[:-2]
    exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
    exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))

    exception_str = "Traceback (most recent call last):\n"
    exception_str += "".join(exception_list)
    # Removing the last \n
    exception_str = exception_str[:-1]

    return exception_str

#----------------------------------------------------------------------------------#