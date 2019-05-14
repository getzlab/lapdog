<template lang="html">
  <div id="methods">
    <div class="modal" id="config-modal">
      <div class="modal-content">
        <div v-if="active_cfg">
          <h4>
            {{active_cfg.config.namespace}}/{{active_cfg.config.name}}
            <span class="right">
              <i v-if="edit_on" v-on:click.prevent="save_config" class="material-icons green-text" title="Save changes">check</i>
              <i class="material-icons" v-bind:class="edit_on?'orange-text text-darken-3':''" v-on:click.prevent="toggle_edit" v-bind:title="edit_on ? 'Discard changes' : 'Edit configuration'">
                {{edit_on?"cancel":"edit"}}
              </i>
              <i class="material-icons red-text" title="Delete this configuration" v-on:click.prevent="prompt_delete">delete</i>
            </span>
          </h4>
          <div style="border-radius: 10px; padding-top: 15px; border: 1px solid grey" class="grey lighten-4">
            <div class="row">
              <div class="col s2">
                Method:
              </div>
              <div class="col s10">
                <a v-bind:href="'https://portal.firecloud.org/#methods/'+active_cfg.config.methodRepoMethod.methodNamespace+'/'+active_cfg.config.methodRepoMethod.methodName+'/'+active_cfg.config.methodRepoMethod.methodVersion" target="_blank" rel="noopener">
                  {{active_cfg.config.methodRepoMethod.methodNamespace}}/{{active_cfg.config.methodRepoMethod.methodName}}:{{active_cfg.config.methodRepoMethod.methodVersion}}
                </a>
              </div>
            </div>
            <div class="row">
              <div class="col s2">
                Entity Type:
              </div>
              <div class="col s9" v-bind:class="active_cfg.config.rootEntityType.match(etype_pattern)?'':'red-text'">
                {{active_cfg.config.rootEntityType}}
              </div>
            </div>
            <div class="row">
              <div class="col s12">
                <a
                v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace+'/method-configs/'+active_cfg.config.namespace+'/'+active_cfg.config.name"
                target="_blank" rel="noopener"
                >
                View config in Firecloud
                </a>
              </div>
            </div>
          </div>
          <div v-if="active_cfg.wdl" style="margin-top: 15px; margin-bottom: 0px;" class="row expandable" v-on:click.prevent="toggle_wdl">
            <div v-if="!show_wdl" class="col s4" >
              <span>
                <i class="material-icons">keyboard_arrow_right</i>
                WDL
              </span>
            </div>
            <div v-else class="col s4">
              <span>
                <i class="material-icons">keyboard_arrow_down</i>
                WDL
              </span>
            </div>
            <div class="col s8 right">

            </div>
          </div>
          <div class="row" v-if="active_cfg.wdl && show_wdl">
            <div style="border-radius: 10px; padding: 15px; border: 1px solid grey" class="log-contianer">
              <pre style="overflow-x: scroll;" v-html="active_cfg.wdl"></pre>
            </div>
          </div>
          <div class="row" style="margin-top: 0px;">
            <div class="col s6">
              <h5>Inputs</h5>
            </div>
            <div class="col s6">
              <h5>Outputs</h5>
            </div>
          </div>
          <div class="row">
            <form class="col s6">
              <div v-if="lodash.has(active_cfg, ['io', 'inputs'])">
                <div class="row" v-for="key in lodash.keys(active_cfg.io.inputs)">
                  <div class="input-field col s11" style="margin: 0px;">
                    <!-- v-bind:class="(lodash.has(active_cfg, ['io', 'inputs', key]) && active_cfg.io.inputs[key].required && active_cfg.config.inputs[key].length < 1) ? 'invalid' : 'valid'" -->
                    <input class="autocomplete" autocomplete="off" type="text" v-bind:id="key" v-model="active_cfg.config.inputs[key]" v-bind:disabled="!edit_on"
                      v-bind:placeholder="(active_cfg.io.inputs[key].required) ? 'Required' : 'Optional'"
                    />
                    <label style="z-index: -1;" v-bind:for="key">
                      {{key + '   ('+active_cfg.io.inputs[key].type+')'}}
                    </label>
                  </div>
                </div>
              </div>
              <div v-else>
                <div class="row" v-for="key in lodash.keys(active_cfg.config.inputs)">
                  <div class="input-field col s11" style="margin: 0px;">
                    <!-- v-bind:class="(lodash.has(active_cfg, ['io', 'inputs', key]) && active_cfg.io.inputs[key].required && active_cfg.config.inputs[key].length < 1) ? 'invalid' : 'valid'" -->
                    <input class="autocomplete" autocomplete="off" type="text" v-bind:id="key" v-model="active_cfg.config.inputs[key]" v-bind:disabled="!edit_on"
                      v-bind:placeholder="(lodash.has(active_cfg, ['io', 'inputs', key]) && active_cfg.io.inputs[key].required) ? 'Required' : 'Optional'"
                    />
                    <label style="z-index: -1;" v-bind:for="key">
                      {{key + (lodash.has(active_cfg, ['io', 'inputs', key]) ? '   ('+active_cfg.io.inputs[key].type+')': '')}}
                    </label>
                  </div>
                </div>
              </div>
            </form>
            <form class="col s6">
              <div v-if="lodash.has(active_cfg, ['io', 'outputs'])">
                <div class="row" v-for="key in lodash.keys(active_cfg.io.outputs)">
                  <div class="input-field col s11" style="margin: 0px;">
                    <input placeholder="Required" type="text" v-bind:id="key" v-model="active_cfg.config.outputs[key]" v-bind:disabled="!edit_on"/>
                    <label style="z-index: -1;">
                      {{key + '   ('+active_cfg.io.outputs[key]+')'}}
                    </label>
                  </div>
                </div>
              </div>
              <div v-else>
                <div class="row" v-for="key in lodash.keys(active_cfg.config.outputs)">
                  <div class="input-field col s11" style="margin: 0px;">
                    <input placeholder="Required" type="text" v-bind:id="key" v-model="active_cfg.config.outputs[key]" v-bind:disabled="!edit_on"/>
                    <label style="z-index: -1;">
                      {{key + (lodash.has(active_cfg, ['io', 'outputs', key]) ? '   ('+active_cfg.io.outputs[key]+')': '')}}
                    </label>
                  </div>
                </div>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
    <div class="modal" id="delete-modal">
      <div class="modal-content">
        <div v-if="active_cfg">
          <h4>
            {{active_cfg.config.namespace}}/{{active_cfg.config.name}}
          </h4>
        </div>
        <strong>Are you sure you want to delete this configuration?</strong>
      </div>
      <div class="modal-footer">
        <div class="row">
          <div class="col s6 offset-s3">
            <a class="red-text left modal-close" v-on:click="delete_config">YES</a>
            <a class="right modal-close">NO</a>
          </div>
        </div>
      </div>
    </div>
    <div class="modal" id="upload-modal">
      <div class="modal-content">
        <h5>Upload new configuration</h5>
        <div class="row">
          <form class="col s6 cfg-form-upload">
            <div class="file-field input-field">
              <div class="btn blue">
                <span>
                  <i class="material-icons">add_circle</i>
                </span>
                <input accept=".json" type="file" v-on:change="(evt) => {config_filepath=evt.target.files[0]}"/>
              </div>
              <div class="file-path-wrapper">
                <input type="text" class="file-path validate" placeholder="Required">
                <label>Method Configuration</label>
              </div>
            </div>
          </form>
          <form class="col s6 cfg-form-upload">
            <div class="file-field input-field">
              <div class="btn blue">
                <span>
                  <i class="material-icons">add_circle</i>
                </span>
                <input type="file" accept=".wdl" v-on:change="(evt) => {method_filepath=evt.target.files[0]}"/>
              </div>
              <div class="file-path-wrapper">
                <input type="text" class="file-path validate" placeholder="Optional">
                <label>Method WDL</label>
              </div>
            </div>
          </form>
        </div>
        <div class="row">
          <div class="col s12">
            <blockquote>
              If a method WDL is uploaded, it's name and namespace will be taken
              from the <code>methodRepoMethod</code> key of the provided configuration. Otherwise,
              the method specified by <code>methodRepoMethod</code> must already exist in Firecloud.
              If <code>methodRepoMethod.methodVersion</code> is set to <code>"latest"</code>, Lapdog will set
              the version to the latest version of the method (including the newly
              uploaded method)
            </blockquote>
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <div class="row">
          <div class="col s6 offset-s3">
            <a class="red-text left modal-close" v-on:click="reset_uploads">CANCEL</a>
            <a class="green-text right" v-on:click="upload_config">UPLOAD</a>
          </div>
        </div>
      </div>
    </div>
    <h4>
      <router-link :to="{name: 'workspace', params: {namespace: namespace, workspace: workspace}}">
        <i class="material-icons tiny">arrow_back</i>  {{namespace}}/{{workspace}}
      </router-link>
    </h4>
    <h3>Methods</h3>
    <div class="row">
      <div class="col s3">
        <a href="#" class="btn blue modal-trigger" data-target="upload-modal" v-on:click.prevent="display_upload">Upload new configuration</a>
      </div>
      <div class="col s5">
        <a target="_blank" rel="noopener" href="https://portal.firecloud.org/#methods" class="btn blue">
          Import existing configuration in Firecloud
        </a>
      </div>
    </div>
    <table v-if="configs">
      <thead>
        <tr>
          <th>Configuration</th>
          <th>Method</th>
          <th>Entity Type</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="config in configs">
          <td>
            <a href="#" v-on:click.prevent="display_config(config.namespace+'/'+config.name, false)">
              {{config.namespace}}/{{config.name}}
            </a>

          </td>
          <td>{{config.methodRepoMethod.methodNamespace}}/{{config.methodRepoMethod.methodName}} (Snapshot {{config.methodRepoMethod.methodVersion}})</td>
          <td v-bind:class="config.rootEntityType.match(etype_pattern) ? '' : 'red-text'">{{config.rootEntityType}}</td>
        </tr>
      </tbody>
    </table>
    <div v-else>
      <div class="row">
        <div class="col s12">
          Loading method configurations...
        </div>
      </div>
      <div class="progress">
        <div class="indeterminate blue"></div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from'axios'
import _ from 'lodash'
export default {
  props: {
    namespace: String,
    workspace: String,
    target_namespace: {
      type: String,
      default: null
    },
    target_name: {
      type: String,
      default: null
    }
  },
  data() {
    return {
      jq:window.$,
      etype_pattern: /(pair|participant|sample)(_set)?/,
      lodash:_,
      configs: null,
      active_cfg: null,
      edit_dirty: null,
      edit_on: false,
      show_wdl: false,
      config_filepath: '',
      method_filepath: '',
      target_rendered: false,
    }
  },
  created() {
    this.init(this.namespace, this.workspace);
    window.$('.modal').modal();
  },
  methods: {
    init(namespace, workspace) {
      this.configs = null;
      this.active_cfg = null;
      this.edit_dirty = null;
      this.edit_on = false;
      this.show_wdl = false;
      this.target_rendered = false;
      this.config_filepath = "";
      this.method_filepath = "";
      this.reset_uploads();
      this.get_configs(namespace, workspace);
    },
    get_configs(ns, ws) {
      axios.get(API_URL+'/api/v1/workspaces/'+ns+'/'+ws+'/configs')
        .then(response => {
          console.log(response.data);
          this.configs = response.data;
          if (this.target_namespace && this.target_name && !this.target_rendered) {
            this.display_config(this.target_namespace+'/'+this.target_name, false);
            // this.target_namespace = null;
            // this.target_name = null;
            this.target_rendered = true;
          }
        })
        .catch(error => {
          console.error(error)
          window.materialize.toast({
            html: "Failed to get method configs"
          })
        })
    },
    toggle_edit() {
      this.edit_on = !this.edit_on;
      if (!this.edit_on) this.discard_changes();
    },
    discard_changes() {
      // //do the shitty version for now
      // window.$("#config-modal").modal('close');
      // this.display_config(
      //   this.active_cfg.config.namespace+'/'+this.active_cfg.config.name,
      //   false
      // );
      axios.get(API_URL + '/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs/'+this.active_cfg.config.namespace+'/'+this.active_cfg.config.name)
        .then(response => {
          console.log(response.data);
          this.active_cfg = response.data;
          setTimeout(() => {
            window.materialize.updateTextFields();
          }, 250)
        })
        .catch(error => {
          console.error(error);
          window.materialize.toast({
            html: "Failed to get config "+config_slug
          });
        })
    },
    save_config() {
      // First check that all outputs are filled, and that if IO is defined, all required inputs exist too
      let error = false;
      _.forIn(this.active_cfg.config.outputs, (value, key) => {
        if (_.trim(value).length == 0) {
          window.materialize.toast({
            html: 'Output "'+key+'" requires a value',
            classes: "red-text"
          })
          error = true;
          return false;
        }
      });
      if (_.has(this.active_cfg, ['io', 'inputs'])) {
        _.forIn(this.active_cfg.config.inputs, (value, key) => {
          let required = _.has(this.active_cfg.io.inputs, key) && this.active_cfg.io.inputs[key].required;
          console.log(key);
          console.log(required);
          if (required && _.trim(value).length == 0) {
            window.materialize.toast({
              html: 'Input "'+key+'" requires a value',
              classes: "red-text"
            })
            error = true;
            return false;
          }
        });
      }
      if (error) return;
      window.materialize.toast({
        html: "Updating configuration",
        displayLength: 1000
      });
      axios.put(
        API_URL + '/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs',
        this.active_cfg.config
      )
        .then(response => {
          this.edit_on = false;
          window.materialize.toast({
            html: "Configuration saved!",
            displayLength: 500
          });
        })
        .catch(error => {
          console.error(error);
          window.materialize.toast({
            html: "Failed to save configuration",
            displayLength: 1000
          })
        })
    },
    prompt_delete() {
      setTimeout(() => {
        window.$("#delete-modal").modal();
        window.materialize.updateTextFields();
        window.$("#delete-modal").modal('open');
      }, 250)
    },
    display_upload() {
      this.reset_uploads();
      window.$("#upload-modal").modal();
      window.materialize.updateTextFields();
      window.$("#upload-modal").modal('open');
    },
    upload_config() {
      if (!this.config_filepath) {
        window.materialize.toast({
          html: "Must provide a method configuration"
        });
        return;
      }
      window.$('#upload-modal').modal();
      window.$('#upload-modal').modal('close');
      window.materialize.toast({
        html: "Uploading configuration...",
        displayLength: 1000
      })
      let data = new FormData();
      data.append('config_filepath', this.config_filepath, this.config_filepath.name);
      if (this.method_filepath) data.append('method_filepath', this.method_filepath, this.method_filepath.name);
      axios.post(
        API_URL + '/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs',
        data
      )
        .then(response => {
          console.log(response.data);
          if (response.data.failed) {
            window.materialize.toast({
              html: 'Unable to update method configuration: '+response.data.reason
            })
          }
          this.reset_uploads();
          window.$('.modal').modal();
          window.$('.modal').modal('close');
          // window.$('.modal').modal('close');
          this.init(this.namespace, this.workspace);
        })
        .catch(error => {
          console.error(error)
          window.materialize.toast({
            html: "Upload error",
            displayLength: 1000
          })
        })
    },
    delete_config() {
      window.materialize.toast({
        html: 'Deleting configuration',
        displayLength: 1000
      });
      axios.delete(API_URL + '/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs/'+this.active_cfg.config.namespace+'/'+this.active_cfg.config.name)
        .then(response => {
          window.$('.modal').modal();
          window.$('.modal').modal('close');
          // window.$('.modal').modal('close');
          this.init(this.namespace, this.workspace);
        })
        .catch(error => {
          console.error(error);
          window.materialize.toast({
            html: 'Unable to delete configuration',
            displayLength: 2000
          })
        })
    },
    reset_uploads() {
      _.forEach(window.$('form.cfg-form-upload'), (elem) => {elem.reset();});
    },
    display_config(config_slug, editable) {
      console.log(config_slug);
      this.active_cfg = null;
      this.edit_dirty = null;
      this.edit_on = editable;
      this.show_wdl = false;
      window.materialize.toast({
        html: "Loading config "+config_slug,
        displayLength: 1000
      });
      axios.get(API_URL + '/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs/'+config_slug)
        .then(response => {
          console.log(response.data);

          if (response.data.config)
          {
            this.active_cfg = response.data;
            if (!response.data.wdl) {
              window.materialize.toast({
                html: "Unable to locate the WDL for this configuration"
              })
            }
            setTimeout(() => {
              window.$("#config-modal").modal();
              window.materialize.updateTextFields();
              window.$("#config-modal").modal('open');
              axios.get(API_URL + '/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs/'+config_slug+'/autocomplete')
                .then(response => {
                  let autocomplete_data = _.reduce(
                      response.data,
                      (obj, key) => {obj[key] = null; return obj},
                      {}
                  );
                  window.$('input.autocomplete').each(
                    (idx, el) => {
                      window.$(el).autocomplete({
                        data: autocomplete_data,
                        limit: 6,
                        onAutocomplete: (choice) => {
                          console.log("Updating field");
                          this.active_cfg.config.inputs[el.id] = choice;
                        }
                      })
                    }
                  )
                })
            }, 250)
          }
          else {
            window.materialize.toast({
              html: "Unable to locate the configuration"
            })
          }


        })
        .catch(error => {
          console.error(error);
          window.materialize.toast({
            html: "Failed to get config "+config_slug
          });
        })
    },
    toggle_wdl() {
      this.show_wdl = !this.show_wdl;
    }
  },
  beforeRouteUpdate(to, from, next) {
    console.log("Update!");
    window.$('.modal').modal('close');
    this.init(to.params.namespace, to.params.workspace);
    next();
  }
}
</script>

<style lang="css" scoped>
  div.log-container {
    max-height: 250px;
    border-radius: 8px;
    overflow-y: auto;
    /* margin: 1em;
    padding: 1em; */
    /* border: 1px solid black; */
    padding-left: 20px;
    font-family: monospace;
    white-space: pre-wrap;
    font-size: 90%;
  }

  .input-field input[type=text].valid {
     border-bottom: 1px solid #000;
     box-shadow: 0 1px 0 0 #000;
   }

   blockquote {
     border-left: 5px solid #42a5f5
   }

   span.type {
     color: #01579b;
   }

   span.keyword, span.section {
     color: #8e24aa;
   }

   span.variable {
     color: #009688;
   }
</style>
