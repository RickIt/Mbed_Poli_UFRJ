#!/usr/bin/env python
#------------------------------------------------------------------------------#
# institution: Escola Politécnica da UFRJ - ITLAB
# instrutor: Diego Dutra, D. Sc.
# disciplina: Modelagem MAP Reduce
# avaliação: trabalho prático 
# grupo: 3
# - Euripedes Antonio Da Silva Junior
# - Victor Hugo Ciurlino
# - Ricardo Nascimento de Souza
# - Pedro Henrique Ferminio Britto
# - Danubia Carvalho Gomes Cantanhede
# program_name: mbed_group3_word_count.py
# objective: a word counter program that receives a filename and use mapreduce
# programming model to return a list of words and ocurrences in the input file
# creation_date: 09/28/2022
# last update: 09/29/2022
#------------------------------------------------------------------------------#
from functools import reduce
import sys
import re

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("filename", help="""file to be word counted, ex: python3 
mbed_group3_word_count.py 'Beyond Good and Evil - Friedrich Nietzsche.txt'""")
parser.parse_args()

def mapper(input_file = sys.argv[1]):
    """
        First step of mapreduce word count. Receives filename as argument, split
        text in words, normalize then in lowercas and remove diacritics
        output a list of key-value pair [k,1].

        MBED description: Recebe um único nome de arquivo e retorna uma 
        sequência de tuplas (palavra, 1)
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        text = f.read()
    
    # Prepare text to avoid case differences and diacritical signals
    lower_case_text = text.lower()
    cleaned_text = re.sub(r"[^a-z\s]+"," ", lower_case_text)
    
    words = cleaned_text.split()
    output = tuple(map(lambda x: (x,1), words))
    
    return output

def partitioner(mapped_words: tuple):
    """
        Second step of mapreduce word count. Sort and aggregate mapped words
        and list his ocurrences preparing for reduce phase.

        MBED description: Recebe como entrada os tuplas do mapper e gera na 
        saída uma lista de tuplas contendo as palavras e uma lista: 
        [(‘BigData’, [1,1] ), (‘map’, [1,1,1])]
    """
    previous_word = ' '
    partitioner_dict = {}

    for word,ocurrence in sorted(mapped_words):
        if word != previous_word:
            partitioner_dict[word] = [ocurrence]
            previous_word = word
        else:
            partitioner_dict[word] += [ocurrence]
    
    result_list = list(partitioner_dict.items())
     
    return result_list

def reducer(partitioner_results: list):
    """
    Third step of mapreduce word count, consists of sum all ocurrences
    from each word from the list received from partitioner phase.
    
    MBED description: Recebe a tupla (‘BigData’, [1,1]) e saída: 
    (palavra, #ocorrências)
    """
    reduced_values = []
    
    for word in partitioner_results:
        count = reduce(lambda x, y: x + y, word[1])
        reduced_values.append(tuple((word[0],count)))

    for value in reduced_values:
        print(value)

if __name__ == "__main__":
    reducer(partitioner(mapper()))
