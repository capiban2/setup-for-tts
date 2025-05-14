class TrieNode {
  key;
  parent;
  children;
  end;

  constructor(key) {
    this.key = key;
    this.parent = null;
    this.children = {};
    this.end = false;
  }

  getWord = function () {
    var output = [];
    var node = this;

    while (node !== null) {
      output.unshift(node.key);
      node = node.parent;
    }

    return output.join('');
  };
}

class Trie {

  root;

  constructor() {
    this.root = new TrieNode(null);
  }
  insert(word) {

    var node = this.root; // we start at the root 😬

    // for every character in the word
    for (var i = 0; i < word.length; i++) {
      // check to see if character node exists in children.
      if (!node.children[word[i]]) {
        // if it doesn't exist, we then create it.
        node.children[word[i]] = new TrieNode(word[i]);

        // we also assign the parent to the child node.
        node.children[word[i]].parent = node;
      }

      // proceed to the next depth in the trie.
      node = node.children[word[i]];

      // finally, we check to see if it's the last word.
      if (i == word.length - 1) {
        // if it is, we set the end flag to true.
        node.end = true;
      }
    }
  }
  contains(word) {

    var node = this.root;

    // for every character in the word
    for (var i = 0; i < word.length; i++) {
      // check to see if character node exists in children.
      if (node.children[word[i]]) {
        // if it exists, proceed to the next depth of the trie.
        node = node.children[word[i]];
      } else {
        // doesn't exist, return false since it's not a valid word.
        return false;
      }
    }

    // we finished going through all the words, but is it a whole word?
    return node.end;
  }

  find(prefix) {

    var node = this.root;
    var output = [];

    // for every character in the prefix
    for (var i = 0; i < prefix.length; i++) {
      // make sure prefix actually has words
      if (node.children[prefix[i]]) {
        node = node.children[prefix[i]];
      } else {
        // there's none. just return it.
        return output;
      }
    }

    // recursively find all words in the node
    this.findAllWords(node, output);

    return output;
  }
  findAllWords(node, arr) {
    // base case, if node is at a word, push to output
    if (node.end) {
      arr.unshift(node.getWord());
    }
  }
};


export { Trie, TrieNode };
