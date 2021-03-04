from setuptools import setup

setup(name='mtr_oeds',
      version='0.1',
      description='python function for OEDS',
      url='',
      author='chenmahh',
      author_email='chenmahh@mtr.com.hk',
      license='OEDS internal only',
      packages=['mtr_oeds','mtr_oeds.drive','mtr_oeds.spirt','mtr_oeds.vsensor','mtr_oeds.common','mtr_oeds.mail'],
      package_data={'':['common/*.csv','spirt/*.*','vsensor/test/*.*','spirt/test/*.*']},
      zip_safe=False)
