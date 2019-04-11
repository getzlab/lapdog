<template>
  <div id="home">
    <div v-if="latest_version && latest_version != lapdog_version" class="row" style="margin-top: 10px; border: 2px solid orange;">
      <div class="col s10 offset s1 pushpin">
        <div class="orange-text">
          Your Lapdog version is out of date. Please upgrade to the latest version with
        </div>
        <code>pip install --upgrade lapdog=={{latest_version}}</code>
      </div>
    </div>
    <div class="row">
      <div class="col s6">
        <h3>
          Firecloud Status
        </h3>
      </div>
    </div>
    <div class="row">
      <div class="col s12 divider">
      </div>
    </div>
    <div class="row">
      <div class="col s12">
        <table>
          <thead>
            <tr>
              <th>System</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="system in systems">
              <td>{{system.system}}</td>
              <td v-bind:class="system.status ? 'green-text' : 'red-text'">
                {{system.status ? "Online" : "Offline"}}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
    <div class="row" v-if="lapdog_version">
      <div class="col s12 center">
        Lapdog version {{lapdog_version}}
      </div>
    </div>
  </div>
</template>

<script type="text/javascript">
  import axios from'axios'
  import _ from 'lodash'
  export default {
    data() {
      return {
        systems: null,
        lapdog_version: null,
        latest_version: null
      }
    },

    created() {
      this.$emit('on-namespace-update', null);
      window.materialize.toast({html:"Bark! Bark!"});
      this.getStatus();
      axios.get(API_URL+'/api/v1/version')
        .then(response => {
          this.lapdog_version = response.data;

          axios.get("https://pypi.org/pypi/lapdog/json")
            .then(response => {
              this.latest_version = _.last(_(response.data.releases).keys().map( version => {
                let components = _.split(version, '.');
                return {
                  major: Number(components[0]),
                  minor: Number(components[1]),
                  patch: Number(components[2]),
                  version: version
                }
              }).sortBy(['major', 'minor', 'patch']).value()).version;
            })

        })
    },

    computed: {
      health() {
        return _.reduce(
          this.systems,
          (current, system) => current && system.status,
          true
        )
      }
    },

    methods: {
      getStatus() {
        axios.get(API_URL+'/api/v1/status')
          .then(response => {
            console.log(response.data);
            if (response.data.failed) window.materialize.toast({
              html: "Unable to query Firecloud status (it may be offline entirely)"
            });
            this.systems = [];
            for(let key in response.data.systems)
            {
              this.systems.push({
                system: key,
                status: response.data.systems[key]
              })
            }
          })
          .catch(error => {
            window.materialize.toast({
              html: "Unable to query Firecloud status (it may be offline entirely)"
            })
          })
      }
    }
  }
</script>
