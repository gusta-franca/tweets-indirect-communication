package classifier;

import java.text.Normalizer;

/* Identifica o tópico de um texto usando palavras-chave. */
public class Classifier{

    private static final String[] TOPICS = {
        "futebol",
        "voleibol",
    };

    private static final String[][] KEYWORDS = {
        { "futebol", "gol", "gols", "escanteio", "pênalti", "penalti", "brasileirao", "libertadores", "campeonato", "atacante", "goleiro" },
        { "volei", "voleibol", "saque", "bloqueio", "levantador", "ponteiro", "libero", "superliga" },
    };

    /* Retorna o tópico com mais palavras-chave encontradas no texto, ou null se nenhuma palavra-chave aparecer (tweet não classificado). */
    public String classify(String text) {
        String normalizedText = normalize(text);
        int bestScore = 0;
        String bestTopic = null;

        for (int i = 0; i < TOPICS.length; i++) {
            int score = 0;

            for (int j = 0; j < KEYWORDS[i].length; j++) {
                String word = normalize(KEYWORDS[i][j]);
                // Garantindo que "gol" não case com "golpe".
                if (normalizedText.contains(" " + word + " ")) {
                    score = score + 1;
                }
            }

            if (score > bestScore) {
                bestScore = score;
                bestTopic = TOPICS[i];
            }
        }

        return bestTopic;
    }

    /* Normaliza o texto em minúsculas, sem acentos, com símbolos trocados por espaço e com um espaço no começo e no fim. */
    private String normalize(String text) {
        String result = text.toLowerCase();
        result = Normalizer.normalize(result, Normalizer.Form.NFD);
        result = result.replaceAll("\\p{M}", "");
        result = result.replaceAll("[^a-z0-9]+", " ");
        return " " + result.trim() + " ";
    }
}