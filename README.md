# Sparkle

## Fonctionnalités

- Traitement d’une liste de données en mémoire
- Nombre illimité d’acteurs
  - Limitation : Pour clusteriser, l’utilisateur de Sparkle doit configurer la sérialisation
- Étape finale de reducing
  - Limitation : Pas de shuffle, la partie reduce se fait sur le master
  - Limitation : Une seule stage à la fin du traitement
- Détection des acteurs défectueux avec la supervision Akka
- Résilience pour l’envoi de messages aux workers

## Lancer le projet

```
sbt "run [options]"
```

**Options possibles :**

- `quiet` : réduit le logging pour éviter de plomber les performances
- `auto-crash` : active les crashs aléatoires des acteurs
- `auto-message-miss` : simule l'échec de réception de messages aléatoirement pour les acteurs 