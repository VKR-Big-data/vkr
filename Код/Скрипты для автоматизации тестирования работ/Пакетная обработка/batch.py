import matplotlib.pyplot as plt

# Фиксированные значения по оси x
x_values = [34650, 69106, 237531]

# Ввод значений по оси y для каждой линии
y_values_mapreduce = [158, 260, 1514]
y_values_spark = [73, 123, 620]
y_values_hive = [87, 144, 701]

# Проверка, что пользователь ввел правильное количество значений
if len(y_values_mapreduce) != 3 or len(y_values_spark) != 3 or len(y_values_hive) != 3:
    print("Пожалуйста, введите по три значения для каждой технологии.")
    exit()

# Построение графика
plt.plot(x_values, y_values_mapreduce, label='MapReduce', marker='o')
plt.plot(x_values, y_values_spark, label='Spark', marker='o')
plt.plot(x_values, y_values_hive, label='Hive', marker='o')

# Настройка легенды
plt.legend()

# Названия осей
plt.xlabel('Количество записей')
plt.ylabel('Время выполнения(с)')

# Название графика
plt.title('Comparison of MapReduce, Spark, and Hive on Tez')

plt.savefig('comparison_graph.png')

# Показ графика
plt.show()
