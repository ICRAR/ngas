******************************************************************************
Recipe for compiling and installing software with or without minitage
******************************************************************************

.. contents::

=======================
Introduction
=======================

Please look for options at http://pypi.python.org/pypi/minitage.recipe.common

The egg has those entry point:

    - *cmmi*: install configure/make/make install softwares

The reasons why i have rewrite yet another buildout recipes builder are:

    - Support for downloading stuff
    - Support on the fly patchs for eggs and other distribution.
    - Support multiple hooks at each stage of the build system.
    - Robust offline mode
    - support automaticly minitage dependencies and rpath linking.

You can browse the code on minitage's following resources:

    - https://github.com/minitage/minitage.recipe.cmmi

You can migrate your buldouts without any effort with buildout.minitagificator:

    - http://pypi.python.org/pypi/buildout.minitagificator


======================================
Makina Corpus sponsored software
======================================
|makinacom|_

* `Planet Makina Corpus <http://www.makina-corpus.org>`_
* `Contact us <mailto:python@makina-corpus.org>`_

  .. |makinacom| image:: http://depot.makina-corpus.org/public/logo.gif
  .. _makinacom:  http://www.makina-corpus.com



