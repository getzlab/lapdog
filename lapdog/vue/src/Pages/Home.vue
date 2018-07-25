<template>
  <div id="home">
    <div class="row">
      <div class="col s12">
        <h3>Firecloud Status</h3>
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
  </div>
</template>

<script type="text/javascript">
  import axios from'axios'
  export default {
    data() {
      return {
        systems: null
      }
    },

    created() {
      this.getStatus();
    },

    methods: {
      getStatus() {
        axios.get('http://localhost:4201/api/v1/status')
          .then(response => {
            console.log(response.data);
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
            console.error("FAIL!")
          })
      }
    }
  }
</script>
