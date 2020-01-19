import sys
import csv
import re
import sqlparse

metadata = {}

def readMetadata():
	global metadata
	tmp_table = []
	flag = False
	fin = open('files/metadata.txt','r')
	for line in fin:
		if flag == True:
			tble = line.strip()
			flag = False
		elif line.strip() == "<begin_table>":
			flag = True;
		elif line.strip() == "<end_table>":
			metadata[tble] = tmp_table
			tmp_table = []
		else:
			tmp_table.append(line.strip())
	#print(metadata)

def getCrossProduct(A, B):
	C = []
	for i in range(len(B)):
		for j in range(len(A)):
			X = []
			X.extend(A[j])
			X.extend(B[i])
			C.append(X)
	#print(C)
	return C

def getTable(par_table_name):
	final_table = []
	tmp_table = []
	for table in par_table_name:
		filename = 'files/' + table + '.csv'
		with open(filename, 'r') as file:
			reader = csv.reader(file)
			for row in reader:
				tmp_table.append(row)
		
		if len(final_table) == 0:
			final_table = tmp_table
		else:
			final_table = getCrossProduct(final_table, tmp_table)
		
		tmp_table = []
	
	return final_table

def modTable(table, par_table_name, cond_token, condition):
	final_table = []
	row = len(table)
	col = len(table[0])

	col_index = findColIndex(par_table_name, cond_token)
	print(col_index)

	for i in range(row):
		res = []
		for j in range(int(len(cond_token)/3)):
			if cond_token[3 * j + 2] == col_index[3 * j + 2]:
				s = str(table[i][col_index[3 * j + 0]]) + col_index[3 * j + 1] + str(col_index[3 * j + 2])
				res.append(eval(s))
			else:
				s = str(table[i][col_index[3 * j + 0]]) + col_index[3 * j + 1] + str(table[i][col_index[3 * j + 2]])
				res.append(eval(s))
				
		#print(res)
		if condition == "AND":
			flag = res[0] and res[1]
		elif condition == "OR":
			flag = res[0] or res[1]
		else:
			flag = res[0]
		
		if flag:
			final_table.append(table[i])
			 

	return final_table

def findColIndex(par_table_name, par_column_name):
	col_index = []
	for k in range(len(par_column_name)):
		s = 0
		if par_column_name[k].find('.') != -1:
			temp = par_column_name[k].split('.')
			for j in range(len(metadata[temp[0]])):
				if metadata[temp[0]][j] == temp[1]:
					for i in range(len(par_table_name)):
						if par_table_name[i] == temp[0]:
							break
						s = s + len(metadata[par_table_name[i]])
					col_index.append(s + j)
		else:
			for i in range(len(par_table_name)):
			#print(metadata[par_table_name[i]])
				flag = False
				for j in range(len(metadata[par_table_name[i]])):			
					if metadata[par_table_name[i]][j] == par_column_name[k]:
						col_index.append(s + j)
						flag = True
						break
				
				if flag == True:
					break
				s = s + len(metadata[par_table_name[i]])
		if len(col_index) == k:
			col_index.append(par_column_name[k])
	return col_index

def findAggregateAndDistinct(par_column_name):
	agg_fun = []
	dist = []
	for i in par_column_name:
		if re.search('max', i, re.IGNORECASE):
			agg_fun.append("max")
		elif re.search('min', i, re.IGNORECASE):
			agg_fun.append("min")
		elif re.search('sum', i, re.IGNORECASE):
			agg_fun.append("sum")
		elif re.search('avg', i, re.IGNORECASE):
			agg_fun.append("avg")
		elif re.search('distinct', i, re.IGNORECASE):
			dist.append('distinct')
	return dist, agg_fun


def findMinimum(table, index):
	row = len(table)
	minimum = 999999999
	for i in range(row):
		minimum = min(minimum, int(table[i][index]))
	return minimum

def findMaximum(table, index):
	row = len(table)
	maximum = -1
	for i in range(row):
		maximum = max(maximum, int(table[i][index]))
	return maximum

def findSum(table, index):
	row = len(table)
	suum = 0
	for i in range(row):
		suum = suum + int(table[i][index])
	return suum

def findAverage(table, index):
	row = len(table)
	suum = 0
	for i in range(row):
		suum = suum + int(table[i][index])
	return suum/row

def findDistinct(table, index):
	freq = {}
	row = len(table)
	for i in range(row):
		freq[table[i][index]] = freq[table[i][index]] + 1
	for i in freq:
		distinct.append(i)
	return distinct

def parseQuery(query):
	parsedQuery = sqlparse.parse(query)[0].tokens
	#print(parsedQuery)
	#sqlparse.parse(query)[0]._pprint_tree()
	tokens = []
	temp = sqlparse.sql.IdentifierList(parsedQuery).get_identifiers()
	
	#print(parsedQuery)
	for i in temp:
		tokens.append(str(i))
	#print(tokens)

	par_table_name = tokens[3].split(',')
	par_column_name = tokens[1].split(',')
	processQuery(tokens, par_table_name, par_column_name)

def processQuery(tokens, par_table_name, par_column_name):
	table = getTable(par_table_name)

	if len(tokens) > 4:
		cond_token = tokens[4][5:len(tokens[4])]
		cond_token = cond_token.strip()
		#print(cond_token)
		condition = "NONE"
		if re.search('and', cond_token, re.IGNORECASE):
			cond_token = cond_token.split()
			cond_token.remove('AND')
			condition = "AND"
		elif re.search('or', cond_token, re.IGNORECASE):
			cond_token = cond_token.split()
			cond_token.remove('OR')
			condition = "OR"
		else:
			cond_token = cond_token.split()

		table = modTable(table, par_table_name, cond_token, condition)

	row = len(table)
	col = len(table[0])
	dist, agg_fun = findAggregateAndDistinct(par_column_name)
	
	if(tokens[1] == '*'):
		for i in par_table_name:
			for j in metadata[i]:
				print(j, end="\t")
		print()
		for i in range(row):
			for j in range(col):
				print(table[i][j], end="\t")
			print()
	elif len(dist) == 1:
		print(par_column_name[0])
		par_column_name[0] = par_column_name[0][9:-1]
		col_index = findColIndex(par_table_name, par_column_name)
		distinct = findDistinct(table, col_index[0])
		for i in distinct:
			print(i)
	elif len(agg_fun) != 0:
		for i in range(len(par_column_name)):
			print(par_column_name[i], end="\t")
			par_column_name[i] = par_column_name[i][4:-1]
		print()
		col_index = findColIndex(par_table_name, par_column_name)
		for i in range(len(agg_fun)):
			if agg_fun[i] == "min":
				print(findMinimum(table, col_index[i]), end="\t")
			elif agg_fun[i] == "max":
				print(findMaximum(table, col_index[i]), end="\t")
			elif agg_fun[i] == "sum":
				print(findSum(table, col_index[i]), end="\t")
			elif agg_fun[i] == "avg":
				print(findAverage(table, col_index[i]), end="\t")
		print()
	else:
		col_index = findColIndex(par_table_name, par_column_name)
		print(col_index)
		for i in par_column_name:
			print(i, end="\t")
		print()
		for i in range(row):
			for j in col_index:
				print(table[i][j], end="\t")
			print()
		print(str(row) + " row affected.")


def main():
	readMetadata()
	query = sys.argv[1]
	#print(query)
	parseQuery(query)

if __name__ == "__main__":
	main()
