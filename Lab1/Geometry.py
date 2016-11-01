import math


def CircleArea(radius):
    if radius < 0:
        print "The radius must be positive"
        return None

    return radius ** 2 * math.pi


def RectangleArea(base, height):
    return base * height
