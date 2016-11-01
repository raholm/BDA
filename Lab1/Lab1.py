# -*- coding: utf-8 -*-

import random
import numpy as np
import math
# from scipy import *   scipy uses numpy
# wenn ich from x import irgendwas schreibe, muss ich nicht mehr mit x.irgendwas darauf
# zugreifen, sondern es geht direkt mit irgendwas





def exercise01():

    parrot = "It is dead, that is what is wrong with it"
    b = len(parrot)

    i = 0
    for x in parrot:
        if x.isalpha():
            i = i + 1


    print(i,b)    # zwei Klammern, weil das eine für print() ist und das andere macht i und b zu einem Tupel

    ParrotWords = parrot.split(" ")

    print(ParrotWords)

    print(" ".join(ParrotWords))


def exercise02():

    for i in range(5,11):    # range() http://pythoncentral.io/pythons-range-function-explained/, da kann man Startwert,
                             # Endwert und auch seq() als Argument übergeben

        print("The next number in the loop is " + str(i))

    """
    geht nicht wie in java, einfach +i, muss i in String umwandeln,
    aber man könnte "text", variable einfügen, dann kann man Variable einfach in print
    abbilden lassen
    """

    #pass   # pass braucht man, wen man nur den Funktionnamen hinschreibt, weil irgednwas in der Funktion drin sein muss
           # wenn man schon etwas drin hat, dann kommt das raus


    while random.uniform(0,1) < 0.9 :

        print("The random number is smaller than 0.9")


    names = ["Ludwig", "Rosa", "Mona", "Amadeus"]

    for name in names:

        print("The name %s is nice" %name)

        """
        mit name in names geht man alle Namen durch, also von Ludwig bis Amadeus
        durch %s wird wie bei Java eine Position für das Einzufügende festgelegt, danach muss aber noch spezifiziert
        werden, was an dieser Stelle eingesetzt werden soll, in dem Fall eben der einzelne Name, der der name aus den
        verfügbaren Namen herausgeholt wird
        """

    # enumerate() gibt zwei Werte zurück, einmal die Indexposition und einmal den Inhalt
    # der Liste, deshalb i und name

    nLetters = [0]*len(names)
    for i,name in enumerate(names):
        nLetters[i] = len(name)

    print(nLetters)


    """
    list comprehension: ist eine Art, eine Liste schneller zu generieren, kein neuer Typ an sich.
    Dadurch spare ich mir die aufwendige Erstellung der Liste, dabei wird erst hingeschriebne, was man machen soll
    (hier eben len(name) und dann die for-Schleife, für was es gilt
    http://www.secnetix.de/olli/Python/list_comprehensions.hawk
    """
    nLetters = [len(name) for name in names]

    print(nLetters)

    """
    In die etablierte Art, eine List comprehension zu erstellen, wird jetzt if-else-Schleife gepackt
    allerdings jetzt ohne Kommas oder sonstige Trennung
    """

    shortLong = ["long" if len(name)>4 else "short"
                 for name in names]

    print(shortLong)


    """
    zip merged im Grunde zwei Listen bzw. die gleichen Indizes für zwei Listen während einer Schleife (nimmt also i-
    test Symbol auf der ersten und i-tes Symbol aus der zweiten Liste), da dann zwei Sachen rauskommen, brauche ich
    in der for-Schleife wieder zwei Variablen, über die ich das Ergebnis anspreche (vgl. enumerate)
    """
    for name,length in zip(names,shortLong):

        print("The name %s is a %s name" %(name, length))   #weil ich jetzt zwei Positionen haben, brauche ich danach
                                                            #eine Klammer (Position ist ausschlaggebend, wenn length
                                                            #vor name, dann wäre der Text andersherum ausgegeben,
                                                            # %s ist nur Platzhalter


def exercise03():

    Amadeus = {'Sex': 'M', 'Algebra': 8, 'History' : 13}   # dictionary = eine Zeile mit eben diesem Inhalt, die ganze Tabelle = Liste von dictionaries

    Rosa = {'Sex': 'F', 'Algebra': 19, 'History': 22}
    Mona = {'Sex': 'F', 'Algebra': 6, 'History': 27}
    Ludwig = {'Sex': 'M', 'Algebra': 9, 'History': 5}

    # Dictionary aus dictionaries machen, wie bisher, nur muss man eben dem Namen des Dictionaries
    # auch das jeweilige dictionary zuweisen (deshalb eben :Amadeus)
    students = {"Amadeus": Amadeus, "Rosa": Rosa, "Mona": Mona, "Ludwig": Ludwig }

    print students["Amadeus"]["History"]

    """
    Neuer Dictionary-Eintrag, in dem ich den "Key" (hier Name) übergebe und dann direkt
    festlege, wie die anderen Atrribute befüllt werden sollen
    """
    students["Karl"] = {"Sex": "M", "Algebra": 14, "History":10}

    for name in students:

        print ("Student %s scored %i on the Algebra exam and %i on the History exam"
               %(name, students[name]["Algebra"], students[name]["History"]))

        """
        Ich will jeweils einen Namen haben, und den brauche ich aus dem Dictionary, deshalb
        for name in students (kein lenght(student), weil man bei dictionaries keinen Index,
        sondern nur den Key hat. Dann wieder Platzhalter rein, %i einfach nur, weil es eine Zahl ist
        (int) und kein string (s), es kommt offenbar ohenhin nur auf das % an. Und dann will ich
        erst den Namen, den bekomme ich schon über name, und dann aber die Note zu dem Namen, deshalb
        muss der Name schon immer noch mit zusätzlich übergeben werden
        """



def exercise04():

    list1 = [1,3,4]
    list2 = [5,6,9]
    # list1 * list2   Multiplication does not work

    array1 = np.array(list1)
    array2 = np.array(list2)      # convertiert list zu array, asarray muss es wohl auch geben

    print array1*array2   # now, it does work

    matrix1 = np.array([array1,array2])  #wenn np.matrix, dann macht er irgendwas, aber nicht was er soll
    matrix2 = np.diag([1,2,3])    # Diagonalelemente der Matrix als Liste übergeben

    #print isinstance(matrix2, np.matrix)   # damit überprüfen, ob die istance das ist, was man will


    #print (matrix1*matrix2)   # doet not work

    print matrix1.dot(matrix2)   # so erfolgreich zwei Matrizen miteiander multiplizieren



def exercise05():

    def CircleArea(radius):

        if radius < 0:
            print "The radius must be positive"
            return None

        # durch return wird der untere Teil der Funktion nie erreicht

        return radius**2 * math.pi

    #print CircleArea(-5) #so called man eine Funktion

    def RectangleArea(base,height):

        return base*height


def main():
    #exercise01()   # wenn ich nur noch eine Aufgabe sehen will, dann in der main-Funktion
    #exercise02()
    #exercise03()
    exercise04()
    exercise05()

if __name__ == "__main__":
    main()
