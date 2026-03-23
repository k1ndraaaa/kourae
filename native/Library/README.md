# About

Nota: este apartado lo dejé suelto porque no sé qué hacer con él. Son algunos objetos que poco a poco iré acomodando en su respectivo contexto.

La intención de este CrossFramework (ahora llamado translators, y se puede encontrar un objeto Request en `commons.py`) es bajar la exclusividad y contexto independiente de cada framework (en este caso Flask, Django, etc) a algo más estandar o Kouraeizado.

El objeto Request de Kourae me puede ayudar demasiado para crear un futuro ExpectedDataForm o otras chingaderas que se me ocurran y me faciliten el desarrollo de mis servicios.

Ahorita hice 3 traductores: django, flask, fastapi porque es lo más usado. Lo siguiente a hacer es una plantilla Traductor que respete el mismo contrato de las 3, así será más escalable construir un traductor.