import random
import math
import numpy as np

def exercise1():
    parrot = "It is dead, that is what is wrong with it."
    print("Parrot: %s" % (parrot,))

    character_count = len(parrot)
    print("Character count: %i" % (character_count,))

    letter_count = sum(char.isalpha() for char in parrot)
    print("Letter count: %i" % (letter_count,))

    words = parrot.split()
    print("Words: %s" % (words,))

    sentence = " ".join(words)
    print("Sentence: %s" % (sentence,))

def exercise2():
    for i in range(5, 11):
        print("The next number in the loop is %i" % (i))

    while random.uniform(0, 1) < 0.9:
        print("The random number is smaller than 0.9")

    names = ["Ludwig", "Rosa", "Mona", "Amadeus"]
    for name in names:
        print("The name %s is nice" % (name,))

    nLetters = []
    for name in names:
        nLetters.append(len(name))

    print("nLetters: %s" % (nLetters,))

    nLetters = [len(name) for name in names]
    print("nLetters: %s" % (nLetters,))

    shortLong = ["long" if len(name) > 4 else "short"
                 for name in names]
    print("shortLong: %s" % (shortLong,))

    for name, desc in zip(names, shortLong):
        print("The name %s is a %s name" % (name, desc))

def exercise3():
    Amadeus = {"Sex": "M", "Algebra": 8, "History": 13}
    Rosa = {"Sex": "F", "Algebra": 19, "History": 22}
    Mona = {"Sex": "F", "Algebra": 6, "History": 27}
    Ludwig = {"Sex": "M", "Algebra": 9, "History": 5}
    print("Amadeus: %s\nRosa: %s\nMona: %s\nLudwig: %s" % (Amadeus, Rosa, Mona, Ludwig,))

    names = ["Amadeus", "Rosa", "Mona", "Ludwig"]
    data = [Amadeus, Rosa, Mona, Ludwig]
    students = {name: d for name, d in zip(names, data)}
    print("Students: %s" % (students,))

    students["Karl"] = {"Sex": "M", "Algebra": 14, "History": 10}
    print("Students: %s" % (students,))

    for student in students:
        print("Student %s scored %i on the Algebra exam and %i on the History exam." %
              (student, students[student]["Algebra"], students[student]["History"]))

def exercise4():
    list1 = [1, 3, 4]
    list2 = [5, 6, 9]
    # list1 * list2 does not work

    array1 = np.array(list1)
    array2 = np.array(list2)
    print("array1 * array2: %s" % (array1 * array2,))

    matrix1 = np.array([list1, list2])
    print("matrix1: %s" % (matrix1))
    matrix2 = np.diag([1, 2, 3])
    print("matrix2: %s" % (matrix2))
    # matrix1 * matrix2 does not work because they have different dimensions (* is element-wise multiplication)
    print("matrix1 * matrix2: %s" % (matrix1.dot(matrix2)))

def exercise5():
    def CircleArea(radius):
        if radius < 0:
            print("Radius must be positive.")
            return None
        return math.pi * math.pow(radius, 2)

    print("Circle Area 0: %f" % (CircleArea(0)))
    print("Circle Area 1: %f" % (CircleArea(1)))
    print("Circle Area 2: %f" % (CircleArea(2)))
    print("Circle Area -1: %s" % (CircleArea(-1)))

    def RectangleArea(base, height):
        if base < 0 or height < 0:
            print("Base/Height must be positive.")
            return None
        return base * height

    print("Rectangle Area (1, 1): %f" % (RectangleArea(1, 1)))
    print("Rectangle Area (2, 3): %f" % (RectangleArea(2, 3)))
    print("Rectangle Area (-1, -2): %s" % (RectangleArea(-1, -2)))

def main():
    exercise1()
    exercise2()
    exercise3()
    exercise4()
    exercise5()

if __name__ == "__main__":
    main()
