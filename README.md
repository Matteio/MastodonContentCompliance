# ***MASTODON CONTENT COMPLIANCE***
***authors:*** Nicola Gabriele, Matteo Capalbo <br>
MASTODON CONTENT COMPLIANCE è un elaborato svolto per l'esame finale del corso di Social Network e media Analysis.<br>

## Descrizione del task svolto
Se consideriamo che Mastodon \`e un social in forte crescita sia per numero di utenti che per diretta conseguenza per numero di post, il compito di
garantire la conformit\`a dei post alle regole stabilite in ciascuna istanza a volte pu\`o diventare arduo. Con questo elaborato ci poniamo l’obiettivo di effettuare un'analisi dei post delle istanze pi\`u rilevanti al fine di quantificare quanto questi siano allineati alle regole dell’istanza in cui sono stati pubblicati. Il seguente lavoro si propone come analisi preliminare per un successivo sviluppo di metodo di rilevazione automatica delle violazioni e/o di proposte di modifica ai post. Scendendo pi\`u nel dettaglio vorremmo servirci di tecnologie come i Large Language Models per analizzare i post delle istanze e ricavare da questi uno score di allineamento per consentirci il calcolo delle statistiche sulla conformit`a dei post e ricavare da esse delle conclusioni. Nel prossimo capitolo segue una descrizione dettagliata della metodologia e delle scelte progettuali.

## Cosa contiene il presente repository
1. Codice python per il crawling dei post delle istanze
2. File json con i risultati del crawling dei post (nella cartella results)
3. File json con gli score di allineamento assegnati da llama3 a ciascun post (nella cartella scores)
4. Diversi notebook che abbiamo utilizzato per effettuare alcuni task al contorno (come il calcolo delle metriche e l'elaborazione dei post attraverso llama)
5. Relazione dettagliata del presente lavoro
<br><br>
***contatti:***<br>
> nicolagabriele1999@gmail.com <br>
> capalbo25@gmail.com

Acknowledgment:
Il lavoro da noi condotto è stato poortato avanti sotto la supervisione del MLN-team del DIMES-UNICAL https://mlnteam-unical.github.io/
