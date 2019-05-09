<template lang="html">
  <div class="preview-root">
    <div class="modal preview-modal" v-bind:id="modal_identifier">
      <div class="modal-content">
        <div v-if="preview" class="container">
          <h4>Object Preview</h4>
          <div class="row">
            <div class="col s3">
              Bucket:
            </div>
            <div class="col s9">
              <a rel="noopener" target="_blank" v-bind:href="'https://console.cloud.google.com/storage/browser/'+preview.bucket">{{preview.bucket}}</a>
            </div>
          </div>
          <div class="preview-body">
            <div v-if="!(preview.size || preview.children.length)">
              <!-- Unable -->
              <div v-if="preview.requesterPays">
                <div class="row">
                  <div class="col s12">
                    Unable to generate preview. Try again with requester pays
                  </div>
                </div>
              </div>
              <div v-else>
                <div class="row">
                  <div class="col s12">
                    Unable to generate preview. You may not have permissions to view this object
                  </div>
                </div>
              </div>
            </div>
            <div v-else>
              <div v-if="preview.children.length > 1 || preview.preview">
                <div class="">
                  <div v-if="preview.children.length" class="attribute-preview ">
                    <div class="row">
                      <div class="col s3">
                        Listing contents of:
                      </div>
                      <div class="col s9" style="font-family: monospace">
                        <span class="grey lighten-2">{{lodash.split(value, preview.bucket)[1]}}</span>
                      </div>
                    </div>
                    <div class="preview-list grey lighten-3 preview-content">
                      <ul class="browser-default">
                        <li v-for="child in preview.children">
                          <span v-bind:title="value+'/'+child">
                            {{child}}
                          </span>
                          <a href="#" v-on:click.prevent="" class="black-text"><i class="material-icons tiny">content_copy</i></a>
                        </li>
                      </ul>
                    </div>
                  </div>
                  <div v-else>
                    <div class="row">
                      <div class="col s4">
                        Displaying first 1kib of:
                      </div>
                      <div class="col s8" style="font-family: monospace">
                        <span class="grey lighten-2">{{lodash.split(value, preview.bucket)[1]}}</span>
                      </div>
                    </div>
                    <div v-if="preview.size" class="row">
                      <div class="col s5">
                        Object Size:
                      </div>
                      <div class="col s7">
                        {{preview.size}}
                      </div>
                    </div>
                    <div class="row preview-container grey lighten-3 attribute-preview preview-content">{{preview.preview}}</div>
                  </div>
                </div>

              </div>
              <div v-else class="row">
                <div class="col s12">
                  Unable to display preview content
                </div>
              </div>
            </div>
          </div>

        </div>
      </div>
      <div class="modal-footer">
        <div v-if="preview">
          <a v-if="preview.visitUrl" class="btn-flat" rel="noopener" target="_blank" v-bind:href="preview.visitUrl">View in Bucket</a>
          <a v-if="preview.url" class="btn-flat" rel="noopener" target="_blank" v-bind:href="preview.url">Download Object</a>
          <a class="modal-close btn-flat">Close Preview</a>
        </div>
      </div>
    </div>
    <!-- First just a fallback if it's not a gs:// path -->
    <span v-if="!lodash.chain(value).toString().startsWith('gs://').value()" class="attribute-preview preview-text preview-display">{{text}}</span>
    <a v-else v-bind:href="value" class="attribute-preview preview-link preview-display" v-on:click.prevent="display">
      {{text}}
    </a>
  </div>


</template>

<script>
import _ from 'lodash';
import axios from'axios';
export default {
  name: 'preview',
  props: {
    value: String,
    text: {
      type: String,
      default: null
    }
  },
  data() {
    return {
      lodash: _,
      modal_identifier: null,
      preview: null
    }
  },
  computed: {
    modal_identifier() {
      return _.toString(this.value)+"-modal"
    }
  },
  methods: {
    close() {
      window.$('#'+_this.modal_identifier).modal('close');
    },
    display() {
      let _this = this;
      //for some stupid reason swagger/connexion can't handle slashes in path components, even if encoded
      let encoded = encodeURIComponent(this.value.replace(/~/g, '~7E')).replace(/%/g, '~');
      axios.get(API_URL+'/api/v1/blob/'+encoded)
        .then((response) => {
          this.preview = response.data;
          window.$('#'+_this.modal_identifier).modal();
          setTimeout(
            () => {
              window.$('#'+_this.modal_identifier).modal('open');
            },
            100
          )
        })
        .catch((error) => {
          window.materialize.toast({html:"Unable to generate preview"});
        })
    }
  },
  created() {
    if (!this.text) this.text = this.value;
    this.modal_identifier = _.uniqueId('preview-instance-')+'-modal';
    window.$('#'+this.modal_identifier).modal();
  }
}
</script>

<style lang="css" scoped>

  div.preview-container {
    /* max-height: 250px; */
    border-radius: 8px;
    /* overflow-y: auto; */
    /* margin: 1em;
    padding: 1em; */
    border: 1px solid black;
    padding-left: 20px;
    font-family: monospace;
    /* white-space: pre-wrap; */
    font-size: 90%;
    overflow-wrap: break-word;
    white-space: pre-wrap;
    width: 100%;
  }

  div.preview-content {
    max-height: 250px;
    overflow-y: auto;

  }

  div.preview-list {
    /* max-height: 250px; */
    border-radius: 8px;
    /* overflow-y: auto; */
    /* margin: 1em;
    padding: 1em; */
    border: 1px solid black;
    padding-left: 20px;
    /* font-family: monospace;
    white-space: pre-wrap; */
    font-size: 90%;
    overflow-x: auto;
  }

</style>
