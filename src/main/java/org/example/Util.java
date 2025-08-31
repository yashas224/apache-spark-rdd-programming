package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Util implements Serializable {
  private Set<String> boringWords;

  public Util() throws IOException {
    boringWords = new HashSet<>();
    loadWords("src/main/resources/subtitles/boringwords.txt");
  }

  private void loadWords(String filePath) throws IOException {
    try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      while((line = reader.readLine()) != null) {
        String word = line.trim();
        if(!word.isEmpty()) {
          boringWords.add(word);
        }
      }
    } catch(Exception e) {
    }
  }

  public Set<String> getBoringWords() {
    return boringWords;
  }

  public boolean isBoringWord(String word) {
    return boringWords.contains(word);
  }
}
