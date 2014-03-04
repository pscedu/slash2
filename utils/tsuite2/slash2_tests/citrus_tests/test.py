import glob, imp

modules = glob.glob('*.py')

modules.remove('test.py')

for module in modules:
    m = __import__(module[:-3])
    print "{} : {} , {} , {}".format(module[:-3], m.setup('/tmp'), m.operate(), m.cleanup())
